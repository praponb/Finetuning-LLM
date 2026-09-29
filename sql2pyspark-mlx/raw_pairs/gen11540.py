from pyspark.sql import functions as F

result = (
    df_app_logs
    .filter(F.col("host") == "h4")
    .select(
        "id",
        "host",
        "payload_json",
        F.shiftright("status_code", 2).alias("status_code_high"),
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["retries"].alias("retry_count"),
        F.array_contains(F.map_keys("context"), "region").alias("has_region"),
        F.get_json_object("payload_json", "$.user.id").alias("user_id"),
    )
)
