from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("host")
w_recent = Window.partitionBy("key", "host")
w3 = Window.partitionBy("key").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-900, Window.currentRow)
w4 = Window.orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-1800, Window.currentRow)

exploded = (
    df_app_logs
    .select(
        "id",
        "host",
        "log_ts",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("key", "attr_value"),
    )
)

cleaned = (
    exploded
    .filter(F.col("log_ts") > F.to_date(F.lit("2025-06-01")))
    .select(
        "id",
        "host",
        "log_ts",
        "latency_ms",
        "status_code",
        "key",
        "attr_value",
        F.greatest("latency_ms", "status_code").alias("greatest_latency_ms_status_code"),
        F.coalesce("key", F.lit("N/A")).alias("key_clean"),
    )
)

scored = (
    cleaned
    .select(
        "*",
        F.stddev_samp("latency_ms").over(w_hist).alias("host_stddev_samp_latency_ms"),
        F.count("greatest_latency_ms_status_code").over(w_recent).alias("key_count_greatest_latency_ms_status_code"),
        F.corr("status_code", "latency_ms").over(w_hist).alias("corr_status_code_latency_ms"),
    )
)

with_windows = (
    scored
    .select(
        "*",
        F.avg("key_count_greatest_latency_ms_status_code").over(w3).alias("key_count_greatest_latency_ms_status_code_avg_15m"),
        F.sum("key_count_greatest_latency_ms_status_code").over(w4).alias("key_count_greatest_latency_ms_status_code_sum_30m"),
    )
)

result = (
    with_windows
    .select(
        "key",
        F.col("greatest_latency_ms_status_code").alias("GREATEST_LATENCY_MS_STATUS_CODE"),
        F.col("corr_status_code_latency_ms").alias("CORR_STATUS_CODE_LATENCY_MS_VAL"),
    )
    .unpivot(
        ["key"],
        ["GREATEST_LATENCY_MS_STATUS_CODE", "CORR_STATUS_CODE_LATENCY_MS_VAL"],
        "kpi",
        "val",
    )
)
