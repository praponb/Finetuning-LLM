from pyspark.sql import functions as F

parsed = (
    df_app_logs
    .filter(F.col("latency_ms").between(1292, 1739))
    .select(
        "id",
        "service",
        "latency_ms",
        "status_code",
        F.coalesce(F.col("context")["version"], F.lit("direct")).alias("version_or_default"),
        (F.col("status_code").bitwiseAND(4) != 0).cast("int").alias("has_fault"),
        F.coalesce(F.col("context")["region"], F.lit("direct")).alias("region_or_default"),
        F.col("context")["trace"].alias("trace"),
    )
)

result = (
    parsed
    .rollup(
        F.when(F.col("latency_ms") >= 2206, "LARGE").otherwise("SMALL").alias("latency_ms_tier"),
    )
    .agg(
        F.percentile_approx("status_code", 0.9).alias("status_code_p90"),
        F.count(F.when(F.col("service") == "search", 1)).alias("search_count"),
        F.round(F.avg("latency_ms"), 2).alias("avg_latency_ms"),
        F.count(F.when(F.col("service") == "auth", 1)).alias("auth_rows"),
        F.countDistinct("service").alias("distinct_services"),
    )
    .filter(F.col("status_code_p90") > 0)
    .orderBy(F.col("avg_latency_ms").asc_nulls_first())
)
