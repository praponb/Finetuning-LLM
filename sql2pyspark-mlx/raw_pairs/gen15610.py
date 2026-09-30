from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("k")

exploded = (
    df_app_logs
    .select(
        "id",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("k", "v"),
    )
)

enriched = exploded.select("*", F.max("latency_ms").over(w).alias("k_max_latency_ms"))

scored = (
    enriched
    .filter(F.col("latency_ms").isNotNull())
    .withColumn("corr_latency_ms_status_code", F.corr("latency_ms", "status_code").over(w))
    .withColumn("corr_status_code_latency_ms", F.corr("status_code", "latency_ms").over(w))
)

cleaned = (
    scored
    .filter(F.col("k").isNotNull())
    .select("*", F.substring("k", 1, 2).alias("k_prefix"))
)

metrics = (
    cleaned
    .select(
        "id",
        "latency_ms",
        "status_code",
        "k",
        "k_max_latency_ms",
        "k_prefix",
        F.sum("corr_status_code_latency_ms").over(w).alias("k_sum_corr_status_code_latency_ms"),
    )
)

result = (
    metrics
    .groupBy("k")
    .agg(
        F.percentile_approx("latency_ms", 0.5).alias("latency_ms_p50"),
        F.round(F.sum("latency_ms") / F.nullif(F.count(F.when(F.col("k") == "trace", 1)), F.lit(0)), 2).alias("latency_ms_per_trace"),
        F.percentile_approx("status_code", [0.25, 0.5, 0.75]).alias("status_code_quartiles"),
        F.round(F.avg("latency_ms"), 2).alias("avg_latency_ms"),
        F.percentile_approx("k_sum_corr_status_code_latency_ms", [0.25, 0.5, 0.75]).alias("k_sum_corr_status_code_latency_ms_quartiles"),
        F.sum(F.when(F.col("k") == "region", F.col("status_code")).otherwise(0)).alias("region_status_code"),
    )
)
