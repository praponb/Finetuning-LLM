from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("k")
w2 = Window.orderBy(F.col("has_fault_adjusted").desc())
w3 = Window.partitionBy("k").orderBy(F.col("has_fault").desc())
w4 = Window.partitionBy("k").orderBy("has_fault")
w5 = Window.partitionBy("k").orderBy(F.col("k_max_has_fault").desc())

exploded = (
    df_app_logs
    .select(
        "id",
        "log_ts",
        "level",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("k", "v"),
    )
)

parsed = (
    exploded
    .select(
        "id",
        "latency_ms",
        "status_code",
        "k",
        "v",
        F.shiftright("status_code", 2).alias("status_code_high"),
        (F.col("status_code").bitwiseAND(2) != 0).cast("int").alias("has_fault"),
    )
)

valid = (
    parsed
    .filter(F.col("k").isNotNull())
    .select(
        "id",
        "status_code",
        "k",
        "v",
        "status_code_high",
        "has_fault",
        F.upper("k").alias("k_upper"),
        F.round((F.col("has_fault") * 0.85), 2).alias("has_fault_adjusted"),
    )
)

with_windows = (
    valid
    .select(
        "id",
        "status_code",
        "k",
        "v",
        "status_code_high",
        "has_fault",
        "k_upper",
        "has_fault_adjusted",
        F.sum("has_fault_adjusted").over(w).alias("k_sum_has_fault_adjusted"),
        F.stddev_samp("status_code_high").over(w).alias("k_stddev_samp_status_code_high"),
        F.cume_dist().over(w2).alias("has_fault_adjusted_cume_dist"),
    )
)

metrics = (
    with_windows
    .select(
        "id",
        "status_code",
        "k",
        "v",
        "status_code_high",
        "k_upper",
        F.min("has_fault").over(w).alias("k_min_has_fault"),
        F.max("has_fault").over(w).alias("k_max_has_fault"),
        F.percent_rank().over(w3).alias("has_fault_percent_rank"),
        F.cume_dist().over(w4).alias("has_fault_cume_dist"),
    )
)

with_windows_app_logs = (
    metrics
    .filter(F.col("k_max_has_fault") >= 0)
    .select("*", F.cume_dist().over(w5).alias("k_max_has_fault_cume_dist"))
)

scored = (
    with_windows_app_logs
    .select(
        "id",
        "status_code",
        "k",
        "k_upper",
        "k_min_has_fault",
        "k_max_has_fault",
        "has_fault_cume_dist",
        "k_max_has_fault_cume_dist",
        F.count("has_fault_percent_rank").over(w).alias("k_count_has_fault_percent_rank"),
    )
)

result = (
    scored
    .select(
        "id",
        "k",
        "k_min_has_fault",
        "k_max_has_fault",
        "has_fault_cume_dist",
        "k_count_has_fault_percent_rank",
    )
    .orderBy(F.col("k_max_has_fault").desc_nulls_first())
)
