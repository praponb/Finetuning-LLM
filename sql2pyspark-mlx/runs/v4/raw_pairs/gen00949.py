from pyspark.sql import functions as F

result = (
    df_app_logs
    .select(
        "id",
        "service",
        "latency_ms",
        "status_code",
        "payload_json",
        "context",
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["retries"].alias("retry_count"),
        F.get_json_object("payload_json", "$.user.tier").alias("user_tier"),
    )
)
