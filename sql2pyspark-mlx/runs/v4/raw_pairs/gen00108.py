from pyspark.sql import functions as F

parsed = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "latency_ms",
        "status_code",
        "context",
        F.get_json_object("payload_json", "$.user.id").alias("user_id"),
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["retries"].alias("retry_count"),
    )
)

result = (
    parsed
    .cube(
        "host",
        F.date_trunc("hour", "log_ts").alias("hour_window"),
        F.when(F.col("status_code") >= 394, "PREMIUM").otherwise("STANDARD").alias("status_code_tier"),
    )
    .agg(
        F.round(F.sum("latency_ms") / F.nullif(F.count(F.when(F.col("host") == "h4", 1)), F.lit(0)), 2).alias("latency_ms_per_h4"),
        ((F.unix_timestamp(F.max("log_ts")) - F.unix_timestamp(F.min("log_ts"))) / 60).alias("span_minutes"),
    )
)
