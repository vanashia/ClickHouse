#include <cstddef>
#include <Server/HTTP/sendExceptionToHTTPClient.h>

#include <IO/WriteHelpers.h>
#include <Server/HTTP/HTTPServerRequest.h>
#include <Server/HTTP/WriteBufferFromHTTPServerResponse.h>
#include <Server/HTTP/exceptionCodeToHTTPStatus.h>
#include <Common/ErrorCodes.h>
#include "Common/Logger.h"
#include "Common/logger_useful.h"
#include "IO/WriteBuffer.h"
#include "base/defines.h"


namespace DB
{

namespace ErrorCodes
{
    extern const int REQUIRED_PASSWORD;
}

void setHTTPResponseStatusAndHeadersForException_A(
    int exception_code, HTTPServerRequest & request, HTTPServerResponse & response, WriteBufferFromHTTPServerResponse * out)
{
    LOG_DEBUG(getLogger("setHTTPResponseStatusAndHeadersForException"), "begin has out {}", bool(out));

    if (out)
        out->setExceptionCode_A(exception_code);
    else
        response.set("X-ClickHouse-Exception-Code", toString<int>(exception_code));

    drainRequstIfNeded(request, response);

    if (exception_code == ErrorCodes::REQUIRED_PASSWORD)
        response.requireAuthentication("ClickHouse server HTTP API");
    else
        response.setStatusAndReason(exceptionCodeToHTTPStatus(exception_code));
}

void drainRequstIfNeded(HTTPServerRequest & request, HTTPServerResponse & response) noexcept
{
    /// If HTTP method is POST and Keep-Alive is turned on, we should try to read the whole request body
    /// to avoid reading part of the current request body in the next request.
    /// Or we have to close connection after this request.
    if (request.getMethod() == Poco::Net::HTTPRequest::HTTP_POST
        && (request.getChunkedTransferEncoding() || request.hasContentLength())
        && response.getKeepAlive())
    {
        try
        {
            if (!request.getStream().eof())
                request.getStream().ignoreAll();
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__, "Cannot read remaining request body during exception handling. Set keep alive to false on the response.");
            response.setKeepAlive(false);
        }
    }
}

}
