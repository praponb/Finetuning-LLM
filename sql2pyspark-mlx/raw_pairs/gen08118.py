from pyspark.sql import functions as F

flattened_attrs = (
    df_app_logs
    .select(
        "id",
        "host",
        "level",
        "latency_ms",
        "payload_json",
        "context",
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["duration"].alias("duration_sec"),
        F.coalesce(F.col("context")["version"], F.lit("none")).alias("version_or_default"),
        F.shiftright("status_code", 2).alias("status_code_high"),
        F.col("context")["region"].alias("region"),
        F.size("context").alias("num_context"),
    )
)

result = (
    flattened_attrs
    .groupBy("level")
    .agg(
        F.count_if(F.col("level") == "ERROR").alias("error_events"),
        F.sum(F.when(F.col("host") == "h1", F.col("status_code_high")).otherwise(0)).alias("h1_status_code_high"),
        F.avg(F.when(F.col("host") == "h1", F.col("duration_sec"))).alias("h1_avg_duration_sec"),
    )
)
