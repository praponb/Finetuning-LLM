from pyspark.sql import functions as F

parsed = (
    df_app_logs
    .filter(F.col("log_ts") >= "2025-08-01")
    .select(
        "id",
        "host",
        "log_ts",
        "status_code",
        "payload_json",
        F.size("context").alias("num_context"),
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["retries"].alias("retry_count"),
        F.array_contains(F.map_keys("context"), "version").alias("has_version"),
    )
)

result = (
    parsed
    .groupBy(
        F.when(F.col("status_code") >= 455, "LARGE").otherwise("SMALL").alias("status_code_tier"),
        F.date_trunc("HOUR", "log_ts").alias("hour_window"),
        "host",
    )
    .agg(
        F.count_if(F.col("host") == "h2").alias("h2_events"),
        F.count("*").alias("num_rows"),
        F.countDistinct("host").alias("distinct_hosts"),
        F.sum(F.when(F.col("host") == "h4", F.col("status_code"))).alias("h4_total_status_code"),
    )
    .orderBy(F.col("status_code_tier").asc_nulls_last())
)
