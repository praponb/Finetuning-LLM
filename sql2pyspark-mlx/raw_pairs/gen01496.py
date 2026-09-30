from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("host").orderBy("latency_ms", "id", "key").rowsBetween(-6, Window.currentRow)

flattened = (
    df_app_logs
    .select(
        "id",
        "host",
        "log_ts",
        "latency_ms",
        "status_code",
        F.explode("context").alias("key", "value"),
    )
)

result = (
    flattened
    .filter(F.col("host") == "h4")
    .select(
        "id",
        "host",
        "key",
        F.min("latency_ms").over(w).alias("last7_min_latency_ms"),
        F.avg("latency_ms").over(w).alias("last7_avg_latency_ms"),
        F.count("latency_ms").over(w).alias("last7_count_latency_ms"),
        F.sum("latency_ms").over(w).alias("last7_sum_latency_ms"),
    )
)
