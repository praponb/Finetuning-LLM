from pyspark.sql import functions as F

parsed = (
    df_app_logs
    .filter(F.col("log_ts") >= "2024-08-10")
    .select(
        "id",
        "host",
        "log_ts",
        F.get_json_object("payload_json", "$.duration").alias("duration_text"),
        F.col("context").getItem("version").alias("version"),
        F.get_json_object("payload_json", "$.user.tier").alias("user_tier"),
        F.coalesce(F.col("context").getItem("version"), F.lit("unknown")).alias("version_or_default"),
    )
)

result = (
    parsed
    .groupBy(F.date_trunc("DAY", "log_ts").alias("activity_day"), "host")
    .agg(
        F.min("log_ts").alias("started_at"),
        F.min("log_ts").alias("started_at_2"),
        F.count("*").alias("event_count"),
    )
    .orderBy(F.col("host").asc_nulls_last())
)
