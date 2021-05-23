from typing import Union, List
from pydruid.client import PyDruid
from pydruid import db

from prefect import Task
from prefect.utilities.tasks import defaults_from_attrs


class DruidQuery(Task):
	"""
	"""

	def __init__(
		self,
		query: str = None,
		fetch: str = "one",
		fetch_count: int = 10,
		host: str = 'localhost',
		port: int = 8082,
		path: str = '/druid/v2/sql/',
		scheme: str = 'http',
		user: str = None,
		password: str = None,
		context: str = None,
		ssl_verify_cert: bool = True,
		ssl_client_cert: str = None,
		**kwargs
	):
		self.query = query
		self.fetch = fetch
		self.fetch_count = 10
		self.host = host
		self.port = port
		self.path = path
		self.scheme = scheme
		self.user = user
		self.password = password
		self.context = context
		self.ssl_verify_cert = ssl_verify_cert
		self.ssl_client_cert = ssl_client_cert
		super().__init__(**kwargs)

	@defaults_from_attrs(
		"query",
		"fetch",
		"fetch_count",
		"host",
		"port",
		"path",
		"scheme",
		"user",
		"password",
		"context",
		"ssl_verify_cert",
		"ssl_client_cert",
	)
	def run(
		self,
		query: str = None,
		fetch: str = "one",
		fetch_count: int = 10,
		host: str = 'localhost',
		port: int = 8082,
		path: str = '/druid/v2/sql/',
		scheme: str = 'http',
		user: str = None,
		password: str = None,
		context: str = None,
		ssl_verify_cert: bool = True,
		ssl_client_cert: str = None,
	):

		if not query:
			raise ValueError("A query string must be provided")

		conn = db.connect(
			host=host,
			port=port,
			path=path,
			scheme=scheme,
			user=user,
			password=password,
			context=context,
			ssl_verify_cert=ssl_verify_cert,
			ssl_client_cert=ssl_client_cert,
		)

		with conn:
			cursor = conn.cursor()
			cursor.execute(query)

			if fetch == "all":
				results = cursor.fetchall()
			elif fetch == "many":
				results = cursor.fetchmany(fetch_count)
			else:
				results = cursor.fetchone()
			return results


class DruidTimeSeries(Task):
	"""
	"""
	def __init__(
		self,
		url: str = 'http://localhost:8082',
		endpoint: str = 'druid/v2',
		cafile: str = None,
		datasource: str = None,
		granularity: str = None,
		intervals: Union[str, List[str]] = None,
		aggregations: dict = None,
		**kwargs,
	):
		self.url = url
		self.endpoint = endpoint
		self.cafile = cafile
		self.datasource = datasource
		self.granularity = granularity
		self.intervals = intervals
		self.aggregations = aggregations
		super().__init__(**kwargs)

	@defaults_from_attrs(
		"url",
		"endpoint",
		"cafile",
		"datasource",
		"granularity",
		"intervals",
		"aggregations",
	)
	def run(
		self,
		url: str = 'http://localhost:8082',
		endpoint: str = 'druid/v2',
		cafile: str = None,
		datasource: str = None,
		granularity: str = None,
		intervals: Union[str, List[str]] = None,
		aggregations: dict = None,
		**druid_kwargs,
	):
		query_client = PyDruid(url=url, endpoint=endpoint, cafile=cafile)

		time_series = query_client.timeseries(
			datasource=datasource,
			granularity=granularity,
			intervals=intervals,
			aggregations=aggregations,
			**druid_kwargs
		)
		return time_series


class DruidTopN(Task):
	"""
	"""
	def __init__(
		self,
		url: str = 'http://localhost:8082',
		endpoint: str = 'druid/v2',
		cafile: str = None,
		datasource: str = None,
		granularity: str = None,
		intervals: Union[str, List[str]] = None,
		aggregations: dict = None,
		dimension: str = None,
		metric: str = None,
		threshold: int = None,
		**kwargs,
	):
		self.url = url
		self.endpoint = endpoint
		self.cafile = cafile
		self.datasource = datasource
		self.granularity = granularity
		self.intervals = intervals
		self.aggregations = aggregations
		self.dimension = dimension,
		self.metric = metric
		self.threshold = threshold
		super().__init__(**kwargs)

	@defaults_from_attrs(
		"url",
		"endpoint",
		"cafile",
		"datasource",
		"granularity",
		"intervals",
		"aggregations",
		"dimension",
		"metric",
		"threshold",
	)
	def run(
		self,
		url: str = 'http://localhost:8082',
		endpoint: str = 'druid/v2',
		cafile: str = None,
		datasource: str = None,
		granularity: str = None,
		intervals: Union[str, List[str]] = None,
		aggregations: dict = None,
		dimension: str = None,
		metric: str = None,
		threshold: int = None,
		**druid_kwargs,
	):
		query_client = PyDruid(url, endpoint, cafile)

		top_n = query_client.topn(
			datasource=datasource,
			granularity=granularity,
			intervals=intervals,
			aggregations=aggregations,
			dimension=dimension,
			metric=metric,
			threshold=threshold,
			**druid_kwargs,
		)


		return top_n

class DruidGroupBy(Task):
	"""
	"""
	def __init__(
		self,
		url: str = 'http://localhost:8082',
		endpoint: str = 'druid/v2',
		cafile: str = None,
		datasource: str = None,
		granularity: str = None,
		intervals: Union[str, List[str]] = None,
		aggregations: dict = None,
		dimensions: str = None,
		**kwargs,
	):
		self.url = url
		self.endpoint = endpoint
		self.cafile = cafile
		self.datasource = datasource
		self.granularity = granularity
		self.intervals = intervals
		self.aggregations = aggregations
		self.dimensions = dimensions
		super().__init__(**kwargs)

	@defaults_from_attrs(
		"url",
		"endpoint",
		"cafile",
		"datasource",
		"granularity",
		"intervals",
		"aggregations",
		"dimensions",
	)
	def run(
		self,
		url: str = 'http://localhost:8082',
		endpoint: str = 'druid/v2',
		cafile: str = None,
		datasource: str = None,
		granularity: str = None,
		intervals: Union[str, List[str]] = None,
		aggregations: dict = None,
		dimensions: List[str] = None,
		**druid_kwargs,
	):
		query_client = PyDruid(url, endpoint, cafile)

		group_by = query_client.groupby(
			datasource=datasource,
			granularity=granularity,
			intervals=intervals,
			aggregations=aggregations,
			dimensions=dimensions,
			**druid_kwargs,
		)

		return group_by