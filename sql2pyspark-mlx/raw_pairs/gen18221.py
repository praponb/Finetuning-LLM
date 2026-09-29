from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level").orderBy(F.col("latency_ms").desc())
w2 = Window.orderBy(F.col("is_flagged").desc_nulls_last())
w3 = Window.partitionBy("service", "level").orderBy(F.col("is_flagged").asc_nulls_first())
w4 = Window.partitionBy("service").orderBy("latency_ms_rounded")
w5 = Window.orderBy(F.col("latency_ms_cume_dist").desc_nulls_last())
w6 = Window.partitionBy("service")

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "level",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("k", "value"),
    )
)

flattened_attrs = (
    exploded
    .select(
        "service",
        "log_ts",
        "level",
        "latency_ms",
        "status_code",
        "value",
        (F.col("status_code").bitwiseAND(2) != 0).cast("int").alias("is_flagged"),
        F.shiftright("status_code", 2).alias("status_code_high"),
    )
)

base = (
    flattened_attrs
    .filter(F.col("log_ts") >= F.lit("2025-06-01").cast("date"))
    .select(
        "service",
        "log_ts",
        "level",
        "latency_ms",
        "status_code",
        "value",
        "is_flagged",
        F.round("latency_ms", 0).alias("latency_ms_rounded"),
        F.substring("service", 1, 2).alias("service_prefix"),
    )
)

with_windows = (
    base
    .withColumn("latency_ms_cume_dist", F.cume_dist().over(w))
    .withColumn("is_flagged_rank", F.rank().over(w2))
    .withColumn("is_flagged_rank_2", F.rank().over(w3))
)

base_app_logs = (
    with_windows
    .filter(F.col("log_ts") > F.to_timestamp(F.lit("2025-06-01 04:00:00")))
    .select("*", F.to_date("log_ts").alias("log_ts_date"))
)

result = (
    base_app_logs
    .filter(F.col("latency_ms").isNotNull())
    .select(
        "service",
        "latency_ms",
        "latency_ms_rounded",
        "service_prefix",
        "latency_ms_cume_dist",
        "is_flagged_rank",
        "log_ts_date",
        F.rank().over(w4).alias("latency_ms_rounded_rank"),
        F.dense_rank().over(w5).alias("latency_ms_cume_dist_dense_rank"),
        F.sum("latency_ms_rounded").over(w6).alias("service_sum_latency_ms_rounded"),
    )
)
