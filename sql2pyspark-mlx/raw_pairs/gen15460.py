from pyspark.sql import functions as F

unpacked = (
    df_app_logs
    .filter(F.col("log_ts") >= "2024-08-01")
    .select(
        "id",
        "host",
        "log_ts",
        "latency_ms",
        "payload_json",
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["user"]["tier"].alias("tier"),
        F.coalesce(F.col("context").getItem("trace"), F.lit("direct")).alias("trace_or_default"),
    )
)

result = (
    unpacked
    .groupBy(
        "host",
        F.when(F.col("latency_ms") >= 1870, "LARGE").otherwise("SMALL").alias("latency_ms_tier"),
        "tier",
    )
    .agg(
        F.count(F.when(F.col("tier") == "free", 1)).alias("free_count"),
        F.count("*").alias("event_count"),
        F.max("latency_ms").alias("max_latency_ms"),
    )
    .filter(F.col("event_count") < 1000)
    .orderBy("tier")
)
