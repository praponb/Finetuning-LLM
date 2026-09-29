from pyspark.sql import functions as F

result = (
    df_app_logs
    .select(
        "id",
        "level",
        "latency_ms",
        "payload_json",
        F.shiftright("status_code", 2).alias("status_code_high"),
        F.array_contains(F.map_keys("context"), "trace").alias("has_trace"),
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["retries"].alias("retry_count"),
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["user"]["tier"].alias("tier"),
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["duration"].alias("duration_sec"),
    )
)
