from pyspark.sql import functions as F

flattened_attrs = (
    df_app_logs
    .filter(F.col("log_ts") >= "2024-09-16")
    .select(
        "id",
        "log_ts",
        "payload_json",
        F.get_json_object("payload_json", "$.user.id").alias("user_id"),
        F.coalesce(F.col("context")["trace"], F.lit("direct")).alias("trace_or_default"),
        F.coalesce(F.col("context")["version"], F.lit("none")).alias("version_or_default"),
        F.get_json_object("payload_json", "$.user.tier").alias("user_tier"),
    )
)

result = (
    flattened_attrs
    .groupBy(F.date_trunc("HOUR", "log_ts").alias("hour_window"))
    .agg(
        F.max("log_ts").alias("ended_at"),
        F.countDistinct("user_tier").alias("distinct_user_tiers"),
    )
    .orderBy(F.col("distinct_user_tiers").desc())
)
