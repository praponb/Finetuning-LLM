from pyspark.sql import functions as F

parsed = (
    df_app_logs
    .select(
        "id",
        "host",
        "level",
        "status_code",
        "payload_json",
        "context",
        F.coalesce(F.col("context")["version"], F.lit("unknown")).alias("version_or_default"),
        F.coalesce(F.col("context").getItem("trace"), F.lit("unknown")).alias("trace_or_default"),
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["duration"].alias("duration_sec"),
    )
)

result = (
    parsed
    .withColumn("duration_sec_tier", F.when(F.col("duration_sec") >= 3.7, "HIGH").otherwise("LOW"))
    .rollup("duration_sec_tier", "host", "level")
    .agg(
        F.percentile_approx("duration_sec", 0.95).alias("duration_sec_p95"),
        F.round(F.avg("status_code"), 2).alias("avg_status_code"),
        F.grouping_id().alias("grouping_level"),
    )
    .orderBy(F.col("duration_sec_p95").desc())
)
