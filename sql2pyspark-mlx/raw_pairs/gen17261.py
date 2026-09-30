from pyspark.sql import functions as F

unpacked = (
    df_app_logs
    .select(
        "id",
        "log_ts",
        "latency_ms",
        "status_code",
        "payload_json",
        "context",
        F.size("context").alias("num_context"),
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["duration"].alias("duration_sec"),
        F.col("context")["trace"].alias("trace"),
    )
)

result = (
    unpacked
    .groupBy(
        F.date_trunc("HOUR", "log_ts").alias("hour_window"),
        F.date_trunc("DAY", "log_ts").alias("activity_day"),
    )
    .agg(
        F.bit_or("status_code").alias("status_code_bit_or"),
        F.avg("num_context").alias("avg_num_context"),
        F.round(F.avg("latency_ms"), 2).alias("avg_latency_ms"),
        F.min("log_ts").alias("first_log_ts"),
    )
    .orderBy(F.col("hour_window").asc_nulls_last())
)
