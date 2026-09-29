from pyspark.sql import functions as F

result = (
    df_app_logs
    .filter(F.col("latency_ms") <= 1227)
    .select(
        "id",
        "host",
        "level",
        "latency_ms",
        F.array_contains(F.map_keys("context"), "region").alias("has_region"),
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["user"]["tier"].alias("tier"),
        F.get_json_object("payload_json", "$.duration").alias("duration_text"),
        F.shiftright("status_code", 2).alias("status_code_high"),
        F.col("context")["trace"].alias("trace"),
    )
)
