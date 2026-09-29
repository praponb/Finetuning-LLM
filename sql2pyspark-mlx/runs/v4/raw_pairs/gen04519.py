from pyspark.sql import functions as F

result = (
    df_app_logs
    .filter(F.col("latency_ms") > 864)
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "level",
        "latency_ms",
        "status_code",
        F.col("context")["version"].alias("version"),
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["duration"].alias("duration_sec"),
        F.col("context")["trace"].alias("trace"),
        F.shiftright("status_code", 2).alias("status_code_high"),
    )
)
