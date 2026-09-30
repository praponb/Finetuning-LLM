from pyspark.sql import functions as F

flattened_attrs = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "latency_ms",
        "context",
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["user"]["tier"].alias("tier"),
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["duration"].alias("duration_sec"),
        F.col("context")["trace"].alias("trace"),
        F.col("context")["region"].alias("region"),
    )
)

result = (
    flattened_attrs
    .groupBy(
        F.when(F.col("duration_sec") >= 4.3, "LARGE").otherwise("SMALL").alias("duration_sec_tier"),
    )
    .agg(
        F.percentile_approx("duration_sec", 0.95).alias("duration_sec_p95"),
        F.avg(F.when(F.col("host") == "h3", F.col("latency_ms"))).alias("h3_avg_latency_ms"),
        F.round(F.sum("latency_ms") / F.nullif(F.count(F.when(F.col("host") == "h3", 1)), F.lit(0)), 2).alias("latency_ms_per_h3"),
        F.bit_or("latency_ms").alias("latency_ms_bit_or"),
        F.bit_and("latency_ms").alias("latency_ms_bit_and"),
    )
    .orderBy(F.col("duration_sec_tier").desc())
)
