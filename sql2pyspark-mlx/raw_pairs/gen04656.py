from pyspark.sql import functions as F

result = (
    df_app_logs
    .filter(F.col("status_code") != 424)
    .select(
        "id",
        "log_ts",
        "level",
        "status_code",
        "context",
        F.col("context")["version"].alias("version"),
        F.coalesce(F.col("context")["trace"], F.lit("none")).alias("trace_or_default"),
        F.coalesce(F.col("context")["region"], F.lit("direct")).alias("region_or_default"),
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["user"]["tier"].alias("tier"),
        F.array_contains(F.map_keys("context"), "trace").alias("has_trace"),
    )
)
