from pyspark.sql import functions as F

result = (
    df_app_logs
    .filter(F.col("level") != "WARN")
    .select(
        "id",
        "service",
        "level",
        "latency_ms",
        "status_code",
        "payload_json",
        "context",
        F.size("context").alias("num_context"),
        F.coalesce(F.col("context").getItem("trace"), F.lit("none")).alias("trace_or_default"),
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["user"]["tier"].alias("tier"),
    )
)
