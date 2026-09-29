from pyspark.sql import functions as F

result = (
    df_app_logs
    .select(
        "id",
        "service",
        "level",
        "status_code",
        "context",
        F.get_json_object("payload_json", "$.duration").alias("duration_text"),
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["duration"].alias("duration_sec"),
        F.size("context").alias("num_context"),
    )
)
