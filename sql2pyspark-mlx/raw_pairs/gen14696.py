from pyspark.sql import functions as F

result = (
    df_app_logs
    .select(
        "id",
        "service",
        "latency_ms",
        "context",
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["duration"].alias("duration_sec"),
        F.coalesce(F.col("context")["region"], F.lit("unknown")).alias("region_or_default"),
        F.col("context")["version"].alias("version"),
        F.col("context")["region"].alias("region"),
        F.get_json_object("payload_json", "$.user.tier").alias("user_tier"),
    )
)
