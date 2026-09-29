from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("k")
w2 = Window.partitionBy("host").orderBy("latency_ms", "id", "k")
w3 = Window.orderBy("latency_ms", "id", "k").rowsBetween(-3, Window.currentRow)
w4 = Window.partitionBy("host")
w5 = Window.partitionBy("k", "host").orderBy("latency_ms", "id", "k").rowsBetween(Window.unboundedPreceding, Window.currentRow)

lines = (
    df_app_logs
    .select("id", "host", "latency_ms", F.explode("context").alias("k", "value"))
)

result = (
    lines
    .select(
        "id",
        "latency_ms",
        "k",
        "value",
        F.round((F.col("latency_ms") / F.sum("latency_ms").over(w)), 4).alias("latency_ms_share"),
        F.ntile(10).over(w2).alias("latency_ms_bucket"),
        F.avg("latency_ms").over(w3).alias("latency_ms_avg_3"),
        F.min("latency_ms").over(w4).alias("host_min_latency_ms"),
        F.last("latency_ms", ignorenulls=True).over(w5).alias("last_known_latency_ms"),
    )
)
