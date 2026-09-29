from pyspark.sql import functions as F

lines = (
    df_app_logs
    .select(
        "id",
        "host",
        "level",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("attr_key", "attr_value"),
    )
)

result = (
    lines
    .groupBy("level")
    .agg(
        F.stddev_samp("latency_ms").alias("latency_ms_stddev"),
        F.avg("latency_ms").alias("avg_latency_ms"),
        F.round(F.sum("status_code") / F.nullif(F.count(F.when(F.col("host") == "h1", 1)), F.lit(0)), 2).alias("status_code_per_h1"),
        F.sum(F.when(F.col("host") == "h4", F.col("status_code")).otherwise(0)).alias("h4_status_code"),
        F.percentile_approx("status_code", 0.95).alias("status_code_p95"),
    )
)
