from pyspark.sql import functions as F

result = (
    df_app_logs
    .filter(F.col("service") == "search")
    .select(
        "id",
        "service",
        "level",
        "latency_ms",
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["user"]["tier"].alias("tier"),
        F.coalesce(F.col("context")["trace"], F.lit("unknown")).alias("trace_or_default"),
        F.coalesce(F.col("context")["version"], F.lit("none")).alias("version_or_default"),
    )
)
