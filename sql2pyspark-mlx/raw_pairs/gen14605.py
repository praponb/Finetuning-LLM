from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-3600, Window.currentRow)
w2 = Window.partitionBy("key")
w3 = Window.partitionBy("key").orderBy(F.col("latency_ms_filled_count_1h").asc_nulls_last())
w4 = Window.partitionBy("service").orderBy(F.col("latency_ms").desc_nulls_last())

flattened = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "level",
        "latency_ms",
        F.explode_outer("context").alias("key", "value"),
    )
)

valid = (
    flattened
    .filter(F.col("key").isNotNull() & (F.col("log_ts") >= F.lit("2025-06-01 12:00:00").cast("timestamp")))
    .select("*", F.coalesce("latency_ms", F.lit(0)).alias("latency_ms_filled"))
)

metrics = (
    valid
    .select(
        "service",
        "log_ts",
        "latency_ms",
        "key",
        F.count("latency_ms_filled").over(w).alias("latency_ms_filled_count_1h"),
    )
)

metrics_app_logs = (
    metrics
    .select(
        "service",
        "latency_ms",
        "key",
        F.sum("latency_ms").over(w2).alias("key_sum_latency_ms"),
        F.rank().over(w3).alias("latency_ms_filled_count_1h_rank"),
        F.cume_dist().over(w4).alias("latency_ms_cume_dist"),
    )
)

result = (
    metrics_app_logs
    .groupBy("service")
    .pivot("key", ["trace", "version", "region"])
    .agg(
        F.max("key_sum_latency_ms").alias("value_sum"),
        F.count(F.lit(1)).alias("item_count"),
    )
)
