//! HTTP QUERY (RFC 10008) over real sockets: the extension method crosses
//! hyper on HTTP/1.1 and HTTP/2, client to server and as raw wire bytes.
#![cfg(all(feature = "client", feature = "server"))]

use std::net::SocketAddr;

use buffa_types::google::protobuf::__buffa::view::StringValueView;
use buffa_types::google::protobuf::StringValue;
use connectrpc::client::{CallOptions, ClientConfig, HttpClient, call_unary, call_unary_query};
use connectrpc::{
    ConnectRpcService, ErrorCode, RequestContext, Router, Server, Spec, StreamType, handler_fn,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

fn router() -> Router {
    Router::new().route_idempotent(
        "svc.Svc",
        "Read",
        handler_fn(|_ctx: RequestContext, req: StringValue| async move {
            connectrpc::Response::ok(StringValue::from(format!("echo {}", req.value)))
        }),
    )
}

async fn serve(service: ConnectRpcService) -> SocketAddr {
    let bound = Server::bind("127.0.0.1:0").await.unwrap();
    let addr = bound.local_addr().unwrap();
    tokio::spawn(bound.serve_with_service(service));
    addr
}

fn spec() -> Spec {
    Spec::client("/svc.Svc/Read", StreamType::Unary)
}

async fn query_call(
    http: &HttpClient,
    addr: SocketAddr,
) -> Result<String, connectrpc::ConnectError> {
    let config = ClientConfig::new(format!("http://{addr}").parse().unwrap());
    let response = call_unary_query::<_, StringValue, StringValueView<'static>>(
        http,
        &config,
        spec(),
        StringValue::from("hi"),
        CallOptions::default(),
    )
    .await?;
    let (_, message, _) = response.into_owned_parts();
    Ok(message.value)
}

#[tokio::test]
async fn client_to_server_over_http1_and_http2() {
    let addr = serve(ConnectRpcService::new(router())).await;
    assert_eq!(
        query_call(&HttpClient::plaintext(), addr).await.unwrap(),
        "echo hi"
    );
    assert_eq!(
        query_call(&HttpClient::plaintext_http2_only(), addr)
            .await
            .unwrap(),
        "echo hi"
    );
}

/// With QUERY disabled the same client call fails while POST still works, so
/// the success above was a QUERY request, not a downgraded POST.
#[tokio::test]
async fn disabled_server_rejects_query_but_serves_post() {
    let addr = serve(ConnectRpcService::new(router()).with_http_query(false)).await;
    for http in [HttpClient::plaintext(), HttpClient::plaintext_http2_only()] {
        let err = query_call(&http, addr).await.unwrap_err();
        assert_ne!(err.code, ErrorCode::Internal);
        let config = ClientConfig::new(format!("http://{addr}").parse().unwrap());
        call_unary::<_, StringValue, StringValueView<'static>>(
            &http,
            &config,
            spec(),
            StringValue::from("hi"),
            CallOptions::default(),
        )
        .await
        .expect("POST still works");
    }
}

#[tokio::test]
async fn raw_http1_query_request_line() {
    let addr = serve(ConnectRpcService::new(router())).await;
    let mut stream = tokio::net::TcpStream::connect(addr).await.unwrap();
    stream
        .write_all(
            b"QUERY /svc.Svc/Read HTTP/1.1\r\nhost: x\r\ncontent-type: application/proto\r\n\
              content-length: 0\r\nconnection: close\r\n\r\n",
        )
        .await
        .unwrap();
    let mut out = Vec::new();
    stream.read_to_end(&mut out).await.unwrap();
    assert!(
        out.starts_with(b"HTTP/1.1 200"),
        "{}",
        String::from_utf8_lossy(&out)
    );
}

#[tokio::test]
async fn raw_http2_query_request() {
    let addr = serve(ConnectRpcService::new(router())).await;
    let tcp = tokio::net::TcpStream::connect(addr).await.unwrap();
    let (mut send, conn) = h2::client::handshake(tcp).await.unwrap();
    tokio::spawn(conn);
    let request = http::Request::builder()
        .method(http::Method::from_bytes(b"QUERY").unwrap())
        .uri(format!("http://{addr}/svc.Svc/Read"))
        .header("content-type", "application/proto")
        .body(())
        .unwrap();
    let (response, _) = send.send_request(request, true).unwrap();
    let response = response.await.unwrap();
    assert_eq!(response.status(), http::StatusCode::OK);
}
