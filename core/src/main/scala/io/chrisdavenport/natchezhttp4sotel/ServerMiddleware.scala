package io.chrisdavenport.natchezhttp4sotel

import cats._
import cats.syntax.all._
import cats.effect.kernel._
import cats.effect.syntax.all._
import org.http4s._
import org.typelevel.ci.CIString
import natchez._
import scala.collection.mutable.ListBuffer
import org.http4s.headers._
import org.http4s.client._
import io.chrisdavenport.natchezhttp4sotel._
import io.chrisdavenport.fiberlocal._
import cats.data.OptionT
import cats.arrow.FunctionK

object ServerMiddleware {

  def default[F[_]: MonadCancelThrow: GenFiberLocal](ep: EntryPoint[F]): ServerMiddlewareBuilder[F] =
    new ServerMiddlewareBuilder[F](ep, Defaults.isKernelHeader, Defaults.reqHeaders, Defaults.respHeaders, Defaults.routeClassifier, Defaults.serverSpanName, Defaults.additionalRequestTags, Defaults.additionalResponseTags, Defaults.includeUrl)

  object Defaults {
    val isKernelHeader: CIString => Boolean = name => !ExcludedHeaders.contains(name)
    val reqHeaders: Set[CIString] = OTHttpTags.Headers.defaultHeadersIncluded
    val respHeaders: Set[CIString] = OTHttpTags.Headers.defaultHeadersIncluded
    def routeClassifier[F[_]]: Request[F] => Option[String] = {(_: Request[F]) => None}
    def serverSpanName[F[_]]: Request[F] => String = {(req: Request[F]) => s"Http Server - ${req.method}"}
    def additionalRequestTags[F[_]]: Request[F] => Seq[(String, TraceValue)] = {(_: Request[F]) => Seq()}
    def additionalResponseTags[F[_]]: Response[F] => Seq[(String, TraceValue)] = {(_: Response[F]) => Seq()}
    def includeUrl[F[_]]: Request[F] => Boolean = {(_: Request[F]) => true}
  }

  final class ServerMiddlewareBuilder[F[_]: MonadCancelThrow: GenFiberLocal] private[ServerMiddleware] (
    ep: EntryPoint[F], 
    isKernelHeader: CIString => Boolean,
    reqHeaders: Set[CIString],
    respHeaders: Set[CIString],
    routeClassifier: Request[F] => Option[String],
    serverSpanName: Request[F] => String,
    additionalRequestTags: Request[F] => Seq[(String, TraceValue)],
    additionalResponseTags: Response[F] => Seq[(String, TraceValue)],
    includeUrl: Request[F] => Boolean,
  ){ self => 

    private def copy(
      ep: EntryPoint[F] = self.ep, 
      isKernelHeader: CIString => Boolean = self.isKernelHeader,
      reqHeaders: Set[CIString] = self.reqHeaders,
      respHeaders: Set[CIString] = self.respHeaders,
      routeClassifier: Request[F] => Option[String] = self.routeClassifier,
      serverSpanName: Request[F] => String = self.serverSpanName,
      additionalRequestTags: Request[F] => Seq[(String, TraceValue)] = self.additionalRequestTags,
      additionalResponseTags: Response[F] => Seq[(String, TraceValue)] = self.additionalResponseTags,
      includeUrl: Request[F] => Boolean = self.includeUrl,
    ): ServerMiddlewareBuilder[F] = 
      new ServerMiddlewareBuilder[F](ep, isKernelHeader, reqHeaders, respHeaders, routeClassifier, serverSpanName, additionalRequestTags, additionalResponseTags, includeUrl)

    def withIsKernelHeader(isKernelHeader: CIString => Boolean) = copy(isKernelHeader = isKernelHeader)
    def withRequestHeaders(reqHeaders: Set[CIString]) = copy(reqHeaders = reqHeaders)
    def withResponseHeaders(respHeaders: Set[CIString]) = copy(respHeaders = respHeaders)
    def withRouteClassifier(routeClassifier: Request[F] => Option[String]) = copy(routeClassifier = routeClassifier)
    def withServerSpanName(serverSpanName: Request[F] => String) = copy(serverSpanName = serverSpanName)
    def withAdditionalRequestTags(additionalRequestTags: Request[F] => Seq[(String, TraceValue)]) = copy(additionalRequestTags = additionalRequestTags)
    def withAdditionalResponseTags(additionalResponseTags: Response[F] => Seq[(String, TraceValue)]) = copy(additionalResponseTags = additionalResponseTags)
    def withIncludeUrl(includeUrl: Request[F] => Boolean) = copy(includeUrl = includeUrl)

    def buildHttpApp(f: Trace[F] => HttpApp[F]): Resource[F, HttpApp[F]] =
      MakeSureYouKnowWhatYouAreDoing.buildTracedF(FunctionK.id)(f.andThen(_.pure[F]))

    def buildHttpRoutes(f: Trace[F] => HttpRoutes[F]): Resource[F, HttpRoutes[F]] =
      MakeSureYouKnowWhatYouAreDoing.buildTracedF(OptionT.liftK)(f.andThen(_.pure[F]))

    final class MakeSureYouKnowWhatYouAreDoing {
      def buildTracedF[G[_]: MonadCancelThrow](fk: F ~> G)(f: Trace[F] => F[Http[G, F]]): Resource[F, Http[G, F]] = {
        // Instantiating a rootSpan, the FiberLocal and the Service outside of the Kleisli, to avoid paying the cost
        // of instantiating the `Http` on a per-request basis.
        ep.root("Root").evalMap { rootSpan =>
          GenFiberLocal[F].local(rootSpan).mproduct(local => f(fromFiberLocal(local))).map { case (local, http) =>
            cats.data.Kleisli[G, Request[F], Response[F]] { (req: Request[F]) =>
              val kernelHeaders = req.headers.headers.collect {
                case header if isKernelHeader(header.name) => header.name.toString -> header.value
              }.toMap
              val kernel = Kernel(kernelHeaders)
              val init = request(req, reqHeaders, routeClassifier, includeUrl) ++ additionalRequestTags(req)
              MonadCancelThrow[G].uncancelable { poll =>
                // Recreating a root using the kernel headers derived from the request.
                ep.continueOrElseRoot(serverSpanName(req), kernel)
                  .mapK(fk)
                  .evalTap { requestSpan =>
                    // Storing the request span in the FiberLocal so that the `Http` has awareness of it
                    // In the current fiber and all descendants
                    fk(local.set(requestSpan))
                  }
                  .use { requestSpan =>
                    fk(requestSpan.put(init: _*)) >> poll(http.run(req)).guaranteeCase {
                      case Outcome.Succeeded(fa) =>
                        fk(requestSpan.put("exit.case" -> "succeeded")) >>
                          fa.flatMap { resp =>
                            val out = response(resp, respHeaders) ++ additionalResponseTags(resp)
                            fk(requestSpan.put(out: _*))
                          }
                      case Outcome.Errored(e) =>
                        fk(requestSpan.put("exit.case" -> "errored")) >>
                          fk(requestSpan.put(OTHttpTags.Errors.error(e): _*))
                      case Outcome.Canceled() =>
                        fk(
                          requestSpan.put(
                            "exit.case" -> "canceled",
                            "canceled" -> true,
                            "error" -> true // A cancelled http is an error for the server. The connection got cut for some reason.
                          )
                        )
                    }
                  }
              }
            }
          }
        }
      }
    }

    def MakeSureYouKnowWhatYouAreDoing = new MakeSureYouKnowWhatYouAreDoing

  }

  private[natchezhttp4sotel] def request[F[_]](req: Request[F], headers: Set[CIString], routeClassifier: Request[F] => Option[String]): List[(String, TraceValue)] = {
    request(req, headers, routeClassifier, Function.const[Boolean, Request[F]](true))
  }

  def request[F[_]](request: Request[F], headers: Set[CIString], routeClassifier: Request[F] => Option[String], includeUrl: Request[F] => Boolean): List[(String, TraceValue)] = {
    val builder = new ListBuffer[(String, TraceValue)]()
    builder += OTHttpTags.Common.kind("server")
    builder += OTHttpTags.Common.method(request.method)
    if (includeUrl(request)) {
      builder += OTHttpTags.Common.url(request.uri)
      builder += OTHttpTags.Common.target(request.uri)
    }
    val host = request.headers.get[Host].getOrElse{
      val key = RequestKey.fromRequest(request)
      Host(key.authority.host.value, key.authority.port)
    }
    builder += OTHttpTags.Common.host(host)
    request.uri.scheme.foreach( s => 
      builder += OTHttpTags.Common.scheme(s)
    )
    request.headers.get[`User-Agent`].foreach( ua => 
      builder += OTHttpTags.Common.userAgent(ua)
    )

    request.contentLength.foreach(l => 
      builder += OTHttpTags.Common.requestContentLength(l)
    )
    routeClassifier(request).foreach(s => 
      builder += OTHttpTags.Server.route(s)
    )
    

    builder += OTHttpTags.Common.flavor(request.httpVersion)

    request.remote.foreach{sa => 
      builder += 
        OTHttpTags.Common.peerIp(sa.host)
      
      builder += 
        OTHttpTags.Common.peerPort(sa.port)
    }
    // Special Server
    request.from.foreach(ip => 
      builder += OTHttpTags.Server.clientIp(ip)
    )
    builder ++= 
      OTHttpTags.Headers.request(request.headers, headers)
    

    builder.toList   
  }

  def response[F[_]](response: Response[F], headers: Set[CIString]): List[(String, TraceValue)] = {
    val builder = new ListBuffer[(String, TraceValue)]()

    builder += OTHttpTags.Common.status(response.status)
    response.contentLength.foreach(l => 
      builder += OTHttpTags.Common.responseContentLength(l)
    )
    builder ++= 
      OTHttpTags.Headers.response(response.headers, headers)
    
    
    builder.toList
  }

  private def fromFiberLocal[F[_]: MonadCancelThrow](local: FiberLocal[F, Span[F]]): natchez.Trace[F] = 
    new natchez.Trace[F] {
      def put(fields: (String, TraceValue)*): F[Unit] =
        local.get.flatMap(_.put(fields: _*))

      def kernel: F[Kernel] =
        local.get.flatMap(_.kernel)

      def span[A](name: String)(k: F[A]): F[A] =
        local.get.flatMap { parent =>
          parent.span(name).flatMap { child =>
            Resource.make(local.set(child))(_ => local.set(parent))
          } .use { _ => k }
        }

      def traceId: F[Option[String]] =
        local.get.flatMap(_.traceId)

      def traceUri =
        local.get.flatMap(_.traceUri)
    }


  val ExcludedHeaders: Set[CIString] = {
    import org.http4s.headers._
    import org.typelevel.ci._

    val payload = Set(
      `Content-Length`.name,
      ci"Content-Type",
      `Content-Range`.name,
      ci"Trailer",
      `Transfer-Encoding`.name,
    )

    val security = Set(
      Authorization.name,
      Cookie.name,
      `Set-Cookie`.name,
    )

    payload ++ security
  }


}
