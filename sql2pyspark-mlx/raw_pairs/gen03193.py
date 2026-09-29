from pyspark.sql import functions as F

flattened = (
    df_app_logs
    .select(
        "id",
        "host",
        "log_ts",
        "level",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("k", "value"),
    )
)

cleaned = (
    flattened
    .filter((F.col("log_ts") >= F.lit("2025-06-01 09:00:00").cast("timestamp")) & (F.col("status_code") >= 401))
    .select(
        "id",
        "log_ts",
        "level",
        "latency_ms",
        "status_code",
        "k",
        "value",
        F.concat("host", F.lit("/"), "level").alias("host_level"),
    )
)

valid = (
    cleaned
    .filter(F.col("status_code") >= 310)
    .withColumn("is_trace", F.when(F.col("k") == "trace", 1).otherwise(0))
)

cleaned_app_logs = (
    valid
    .filter(F.col("log_ts") >= F.to_date(F.lit("2025-06-01")))
    .select(
        "*",
        F.round("is_trace", 2).alias("is_trace_rounded"),
        F.col("latency_ms").cast("double").alias("latency_ms_str"),
        F.coalesce("k", F.lit("NONE")).alias("resolved_k"),
    )
)

valid_app_logs = (
    cleaned_app_logs
    .filter(F.col("latency_ms_str") <= 58)
    .select(
        "id",
        "log_ts",
        "level",
        "latency_ms",
        "status_code",
        "k",
        "host_level",
        "is_trace",
        "is_trace_rounded",
        "latency_ms_str",
        "resolved_k",
        F.ceil("status_code").alias("status_code_whole"),
        F.coalesce("k", F.lit("N/A")).alias("resolved_k_2"),
    )
)

result = (
    valid_app_logs
    .groupBy(F.date_trunc("DAY", "log_ts").alias("activity_day"))
    .agg(
        F.max("log_ts").alias("ended_at"),
        F.sum(F.when(F.col("level") == "INFO", F.col("status_code")).otherwise(0)).alias("info_status_code"),
        F.count(F.when(F.col("k") == "trace", 1)).alias("trace_rows"),
        F.sum(F.when(F.col("k") == "trace", F.col("latency_ms"))).alias("trace_total_latency_ms"),
    )
    .orderBy(F.col("activity_day").desc_nulls_last())
)
