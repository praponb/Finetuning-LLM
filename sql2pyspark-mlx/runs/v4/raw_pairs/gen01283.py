from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("service").orderBy("latency_ms")
w2 = Window.partitionBy("host").orderBy("log_ts", "id", "k").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy("latency_ms", "id", "k")
w4 = Window.partitionBy("k").orderBy(F.col("latency_ms").desc(), "id", "k")

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "latency_ms",
        F.explode("context").alias("k", "value"),
    )
)

result = (
    exploded
    .select(
        "*",
        F.percent_rank().over(w).alias("latency_ms_percent_rank"),
        F.first("k", ignorenulls=True).over(w2).alias("first_known_k"),
        F.ntile(4).over(w3).alias("latency_ms_bucket"),
        F.first("latency_ms", ignorenulls=True).over(w2).alias("first_known_latency_ms"),
        F.ntile(10).over(w4).alias("latency_ms_bucket_2"),
    )
)
