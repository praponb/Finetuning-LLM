from pyspark.sql import functions as F

parsed = (
    df_app_logs
    .filter(F.col("level") == "INFO")
    .select(
        "id",
        "service",
        "level",
        "context",
        F.col("context")["version"].alias("version"),
        F.coalesce(F.col("context")["trace"], F.lit("direct")).alias("trace_or_default"),
        F.size("context").alias("num_context"),
        F.array_contains(F.map_keys("context"), "region").alias("has_region"),
        F.col("context")["region"].alias("region"),
    )
)

result = (
    parsed
    .groupBy("level")
    .agg(
        F.avg(F.when(F.col("level") == "DEBUG", F.col("num_context"))).alias("debug_avg_num_context"),
        F.percentile_approx("num_context", 0.5).alias("num_context_p50"),
        F.count(F.when(F.col("level") == "INFO", 1)).alias("info_count"),
        F.sum(F.when(F.col("service") == "checkout", F.col("num_context")).otherwise(0)).alias("checkout_num_context"),
        F.sum(F.when(F.col("service") == "auth", F.col("num_context"))).alias("auth_total_num_context"),
    )
    .filter(F.col("auth_total_num_context") > 0)
)
