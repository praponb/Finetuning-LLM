from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-3600, Window.currentRow)
w_running = Window.orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-1800, Window.currentRow)
w_running_2 = Window.partitionBy(F.month("log_ts"))
w4 = Window.orderBy("status_code_high")

flattened = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "level",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("attr_key", "v"),
    )
)

parsed = (
    flattened
    .filter(F.col("log_ts") < "2023-11-01")
    .select(
        "id",
        "log_ts",
        "latency_ms",
        (F.col("status_code").bitwiseAND(4) != 0).cast("int").alias("has_fault"),
        F.shiftright("status_code", 2).alias("status_code_high"),
    )
)

metrics = (
    parsed
    .select(
        "*",
        F.avg("has_fault").over(w).alias("has_fault_avg_1h"),
        F.count("has_fault").over(w_running).alias("has_fault_count_30m"),
        F.stddev_samp("has_fault").over(w_running_2).alias("log_ts_month_stddev_samp_has_fault"),
        F.dense_rank().over(w4).alias("status_code_high_dense_rank"),
    )
)

filtered = (
    metrics
    .filter(F.col("log_ts") > F.lit("2025-06-01 12:00:00").cast("timestamp"))
    .select("*", F.coalesce("has_fault", F.lit(0)).alias("has_fault_filled"))
)

result = (
    filtered
    .select("id", "has_fault_count_30m", "log_ts_month_stddev_samp_has_fault")
    .orderBy(F.col("log_ts_month_stddev_samp_has_fault").desc_nulls_last())
)
