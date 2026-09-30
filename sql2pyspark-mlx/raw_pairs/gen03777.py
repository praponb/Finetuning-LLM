from pyspark.sql import functions as F

result = (
    df_app_logs
    .filter(F.col("host").isin(["h1", "h4"]))
    .select(
        "id",
        "service",
        "host",
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["retries"].alias("retry_count"),
        F.shiftright("status_code", 2).alias("status_code_high"),
        F.col("context")["version"].alias("version"),
        F.array_contains(F.map_keys("context"), "version").alias("has_version"),
    )
)
