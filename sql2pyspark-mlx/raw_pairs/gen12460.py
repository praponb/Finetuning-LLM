from pyspark.sql import functions as F

flattened_attrs = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "latency_ms",
        "context",
        F.coalesce(F.col("context")["region"], F.lit("direct")).alias("region_or_default"),
        (F.col("status_code").bitwiseAND(2) != 0).cast("int").alias("is_flagged"),
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["duration"].alias("duration_sec"),
        F.coalesce(F.col("context")["version"], F.lit("none")).alias("version_or_default"),
    )
)

result = (
    flattened_attrs
    .rollup(
        F.when(F.col("is_flagged") >= 1, "PREMIUM").otherwise("STANDARD").alias("is_flagged_tier"),
    )
    .agg(
        F.count(F.when(F.col("service") == "search", 1)).alias("search_count"),
        F.size(F.collect_set("host")).alias("num_hosts"),
        F.bit_and("latency_ms").alias("latency_ms_bit_and"),
        F.percentile_approx("duration_sec", 0.9).alias("duration_sec_p90"),
    )
    .filter(F.col("search_count") > 1)
    .orderBy("search_count")
)
