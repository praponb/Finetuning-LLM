from pyspark.sql import functions as F

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "level",
        "latency_ms",
        F.explode_outer("context").alias("key", "v"),
    )
)

result = (
    exploded
    .groupBy("level")
    .agg(
        F.percentile_approx("latency_ms", 0.5).alias("latency_ms_p50"),
        F.count(F.when(F.col("level") == "INFO", 1)).alias("info_count"),
        F.sort_array(F.collect_set("key")).alias("key_list"),
        F.sum(F.when(F.col("level") == "INFO", F.col("latency_ms")).otherwise(0)).alias("info_latency_ms"),
        F.count(F.when(F.col("service") == "search", 1)).alias("search_count"),
    )
    .orderBy(F.col("info_count").desc())
)
