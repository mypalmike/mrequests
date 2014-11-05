"""
mrequests

This is a modification of grequests where the underlying library is multiprocessing
rather than gevent. It enables use of the requests library with parallelism.

IMPORTANT NOTE:

Due to the way python's multiprocessing library works, you can **NOT** use
these functions from an interactive python session. See note on
https://docs.python.org/2/library/multiprocessing.html,
specifically, "Functionality within this package requires that the __main__ module
be importable by the children."

In my experience, things hang and lots of extra processes start up if you try
calling either map() or imap() from an interactive python session.
"""

from functools import partial
from multiprocessing import Pool
from requests import Session


__all__ = (
    'map', 'imap',
    'get', 'options', 'head', 'post', 'put', 'patch', 'delete', 'request'
)


class AsyncRequest(object):
    """
    Asynchronous request.

    Accept same parameters as ``Session.request`` and some additional:

    :param session: Session which will do request
    :param callback: Callback called on response.
                     Same as passing ``hooks={'response': callback}``
    """
    def __init__(self, method, url, **kwargs):
        #: Request method
        self.method = method
        #: URL to request
        self.url = url
        #: Associated ``Session``
        self.session = kwargs.pop('session', None)
        if self.session is None:
            self.session = Session()

        callback = kwargs.pop('callback', None)
        if callback:
            kwargs['hooks'] = {'response': callback}

        #: The rest arguments for ``Session.request``
        self.kwargs = kwargs

        #: Resulting ``Response``
        self.response = None

    def send(self, **kwargs):
        """
        Prepares request based on parameter passed to constructor and optional ``kwargs```.
        Then sends request and saves response to :attr:`response`

        :returns: ``Response``
        """
        merged_kwargs = {}
        merged_kwargs.update(self.kwargs)
        merged_kwargs.update(kwargs)
        try:
            self.response = self.session.request(self.method, self.url, **merged_kwargs)
        except Exception as e:
            self.exception = e
        return self


def send(r, stream=False):
    return r.send(stream=stream)


# Shortcuts for creating AsyncRequest with appropriate HTTP method
get = partial(AsyncRequest, 'GET')
options = partial(AsyncRequest, 'OPTIONS')
head = partial(AsyncRequest, 'HEAD')
post = partial(AsyncRequest, 'POST')
put = partial(AsyncRequest, 'PUT')
patch = partial(AsyncRequest, 'PATCH')
delete = partial(AsyncRequest, 'DELETE')

# synonym
def request(method, url, **kwargs):
    return AsyncRequest(method, url, **kwargs)


def map(requests, stream=False, size=None, exception_handler=None):
    """Concurrently converts a list of Requests to Responses.

    :param requests: a collection of Request objects.
    :param stream: If True, the content will not be downloaded immediately.
    :param size: Specifies the number of requests to make at a time. If None, uses default pool size of
                 multiprocessing, which is the number of CPUs.
    :param exception_handler: Callback function, called when exception occured. Params: Request, Exception
    """

    requests = list(requests)
    pool = Pool(processes=size) if size else Pool()
    requests = pool.map(partial(send, stream=stream), requests)
    pool.close()
    pool.join()

    ret = []

    for request in requests:
        if request.response:
            ret.append(request.response)
        elif exception_handler:
            exception_handler(request, request.exception)

    return ret


def imap(requests, stream=False, size=5, exception_handler=None):
    """
    Concurrently converts a generator object of Requests to
    a generator of Responses.

    :param requests: a generator of Request objects.
    :param stream: If True, the content will not be downloaded immediately.
    :param size: Specifies the number of requests to make at a time. default is 2
    :param exception_handler: Callback function, called when exception occured. Params: Request, Exception
    """

    pool = Pool(processes=size)

    for request in pool.imap_unordered(partial(send, stream=stream), requests):
        if request.response:
            yield request.response
        elif exception_handler:
            exception_handler(request, request.exception)

    pool.close()
    pool.join()

