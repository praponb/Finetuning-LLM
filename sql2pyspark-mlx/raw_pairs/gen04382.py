from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-900, Window.currentRow)
w2 = Window.partitionBy("level")
w3 = Window.partitionBy(F.date_trunc("MONTH", "log_ts"), "level")
w4 = Window.partitionBy("host")
w5 = Window.partitionBy(F.to_date("log_ts")).orderBy(F.col("latency_ms_share").desc_nulls_first())
w6 = Window.partitionBy("service").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-3600, Window.currentRow)

flattened = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "level",
        "latency_ms",
        F.explode_outer("context").alias("k", "value"),
    )
)

valid = (
    flattened
    .filter(F.col("k").isNotNull() & (F.col("log_ts") >= F.to_date(F.lit("2025-06-01"))))
    .withColumn("latency_ms_tier", F.when(F.col("latency_ms") >= 1875, "High").when(F.col("latency_ms") >= 1039, "Medium").otherwise("Low"))
    .withColumn("k_clean", F.coalesce("k", F.lit("OTHER")))
)

metrics = (
    valid
    .filter(F.col("log_ts") >= "2023-09-01")
    .select(
        "*",
        F.avg("latency_ms").over(w).alias("latency_ms_avg_15m"),
        F.round((F.col("latency_ms") / F.sum("latency_ms").over(w2)), 4).alias("latency_ms_share"),
        F.round((F.col("latency_ms") / F.sum("latency_ms").over(w3)), 4).alias("latency_ms_share_2"),
    )
)

metrics_app_logs = (
    metrics
    .select(
        "*",
        F.max("latency_ms_share_2").over(w4).alias("host_max_latency_ms_share_2"),
        F.cume_dist().over(w5).alias("latency_ms_share_cume_dist"),
        F.sum("latency_ms_share").over(w6).alias("latency_ms_share_sum_1h"),
    )
)

result = (
    metrics_app_logs
    .groupBy(
        F.when(F.col("latency_ms_share_2") >= 0.72, "LARGE").otherwise("SMALL").alias("latency_ms_share_2_tier"),
        F.date_trunc("DAY", "log_ts").alias("activity_day"),
        F.when(F.col("host_max_latency_ms_share_2") >= 1, "PREMIUM").otherwise("STANDARD").alias("host_max_latency_ms_share_2_tier"),
    )
    .agg(
        ((F.unix_timestamp(F.max("log_ts")) - F.unix_timestamp(F.min("log_ts"))) / 60).alias("span_minutes"),
        ((F.unix_timestamp(F.max("log_ts")) - F.unix_timestamp(F.min("log_ts"))) / 60).alias("span_minutes_2"),
        F.avg("latency_ms_share_2").alias("avg_latency_ms_share_2"),
    )
    .orderBy("activity_day")
)
