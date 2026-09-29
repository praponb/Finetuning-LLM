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
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["retries"].alias("retry_count"),
        F.col("context")["version"].alias("version"),
        F.coalesce(F.col("context").getItem("trace"), F.lit("none")).alias("trace_or_default"),
    )
)
