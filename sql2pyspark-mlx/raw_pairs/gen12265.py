from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("service").orderBy("latency_ms", "id", "attr_key").rowsBetween(-2, -1)
w_hist = Window.partitionBy("level").orderBy(F.col("latency_ms").desc(), "id", "attr_key")
w_ordered = Window.orderBy("latency_ms", "id", "attr_key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_hist_2 = Window.partitionBy("host").orderBy("latency_ms", "id", "attr_key")

flattened = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "level",
        "latency_ms",
        F.explode("context").alias("attr_key", "value"),
    )
)

result = (
    flattened
    .filter(F.col("level").isin(["INFO", "DEBUG", "WARN"]))
    .select(
        "id",
        "service",
        "host",
        "latency_ms",
        "attr_key",
        "value",
        F.avg("latency_ms").over(w).alias("latency_ms_avg_2_prior"),
        F.ntile(5).over(w_hist).alias("latency_ms_bucket"),
        F.sum("latency_ms").over(w_ordered).alias("cumulative_latency_ms"),
        F.lag("latency_ms", 2).over(w_hist_2).alias("prev_latency_ms"),
    )
)
