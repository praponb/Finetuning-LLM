from pyspark.sql import functions as F

result = (
    df_app_logs
    .select(
        "id",
        "host",
        "log_ts",
        "latency_ms",
        "status_code",
        "payload_json",
        "context",
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["duration"].alias("duration_sec"),
        F.get_json_object("payload_json", "$.user.id").alias("user_id"),
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["user"]["tier"].alias("tier"),
        F.coalesce(F.col("context").getItem("region"), F.lit("none")).alias("region_or_default"),
        F.col("context")["region"].alias("region"),
    )
)
