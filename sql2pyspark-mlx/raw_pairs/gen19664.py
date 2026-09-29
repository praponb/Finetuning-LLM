from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("service").orderBy("has_fault")
w2 = Window.orderBy(F.col("status_code_high_plus_has_fault").desc())
w3 = Window.partitionBy("service")
w_hist_2 = Window.partitionBy("service").orderBy(F.col("has_fault_dense_rank").asc_nulls_last())

flattened = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("attr_key", "v"),
    )
)

unpacked = (
    flattened
    .select(
        "id",
        "service",
        "log_ts",
        "latency_ms",
        "v",
        F.shiftright("status_code", 2).alias("status_code_high"),
        (F.col("status_code").bitwiseAND(4) != 0).cast("int").alias("has_fault"),
    )
)

cleaned = (
    unpacked
    .filter((F.col("log_ts") > F.lit("2025-06-01").cast("date")) & (F.col("latency_ms") != 1200))
    .select(
        "service",
        "log_ts",
        "v",
        "has_fault",
        (F.datediff(F.current_date(), "log_ts") / 7).alias("weeks_since_log_ts"),
        (F.col("status_code_high") + F.col("has_fault")).alias("status_code_high_plus_has_fault"),
    )
)

scored = (
    cleaned
    .select(
        "service",
        "status_code_high_plus_has_fault",
        F.dense_rank().over(w).alias("has_fault_dense_rank"),
        F.rank().over(w2).alias("status_code_high_plus_has_fault_rank"),
        F.stddev_samp("has_fault").over(w3).alias("service_stddev_samp_has_fault"),
    )
)

enriched = (
    scored
    .filter(F.col("status_code_high_plus_has_fault_rank") < 8)
    .select(
        "status_code_high_plus_has_fault",
        "has_fault_dense_rank",
        "status_code_high_plus_has_fault_rank",
        "service_stddev_samp_has_fault",
        F.max("status_code_high_plus_has_fault_rank").over(w3).alias("service_max_status_code_high_plus_has_fault_rank"),
        F.round((F.col("has_fault_dense_rank") / F.sum("has_fault_dense_rank").over(w3)), 4).alias("has_fault_dense_rank_share"),
        F.percent_rank().over(w_hist_2).alias("has_fault_dense_rank_percent_rank"),
        F.max("status_code_high_plus_has_fault_rank").over(w3).alias("service_max_status_code_high_plus_has_fault_rank_2"),
        F.max("status_code_high_plus_has_fault_rank").over(w3).alias("service_max_status_code_high_plus_has_fault_rank_3"),
    )
)

base = (
    enriched
    .filter(F.col("status_code_high_plus_has_fault") < 33)
    .select(
        "status_code_high_plus_has_fault",
        "service_stddev_samp_has_fault",
        "service_max_status_code_high_plus_has_fault_rank",
        "has_fault_dense_rank_share",
        "service_max_status_code_high_plus_has_fault_rank_3",
        (F.col("service_max_status_code_high_plus_has_fault_rank") + F.col("has_fault_dense_rank_share")).alias("service_max_status_code_high_plus_has_fault_rank_plus_has_fault_dense_rank_share"),
        F.floor("service_max_status_code_high_plus_has_fault_rank").alias("service_max_status_code_high_plus_has_fault_rank_whole"),
    )
)

valid = (
    base
    .filter(F.col("status_code_high_plus_has_fault").between(32, 75))
    .select(
        "*",
        F.greatest("service_max_status_code_high_plus_has_fault_rank_whole", "service_stddev_samp_has_fault").alias("greatest_service_max_status_code_high_plus_has_fault_rank_whole_service_stddev_samp_has_fault"),
        F.coalesce("has_fault_dense_rank_share", F.lit(0)).alias("has_fault_dense_rank_share_filled"),
    )
)

result = (
    valid
    .select(
        "service_stddev_samp_has_fault",
        "service_max_status_code_high_plus_has_fault_rank",
        "has_fault_dense_rank_share",
        "service_max_status_code_high_plus_has_fault_rank_plus_has_fault_dense_rank_share",
        "service_max_status_code_high_plus_has_fault_rank_whole",
        "greatest_service_max_status_code_high_plus_has_fault_rank_whole_service_stddev_samp_has_fault",
    )
    .orderBy(
        "greatest_service_max_status_code_high_plus_has_fault_rank_whole_service_stddev_samp_has_fault",
    )
)
