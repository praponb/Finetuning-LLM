from pyspark.sql import functions as F

unpacked = (
    df_app_logs
    .filter(F.hour("log_ts").between(10, 16))
    .select(
        "id",
        "host",
        "log_ts",
        "latency_ms",
        "status_code",
        F.coalesce(F.col("context").getItem("version"), F.lit("direct")).alias("version_or_default"),
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["retries"].alias("retry_count"),
        F.get_json_object("payload_json", "$.duration").alias("duration_text"),
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["user"]["tier"].alias("tier"),
        F.coalesce(F.col("context")["trace"], F.lit("unknown")).alias("trace_or_default"),
    )
)

result = (
    unpacked
    .rollup("tier")
    .agg(
        F.percentile_approx("status_code", 0.95).alias("status_code_p95"),
        F.max("log_ts").alias("ended_at"),
        F.count(F.when(F.col("tier") == "pro", 1)).alias("pro_rows"),
        F.grouping_id().alias("grouping_id"),
    )
    .orderBy(F.col("status_code_p95").desc_nulls_first())
)
