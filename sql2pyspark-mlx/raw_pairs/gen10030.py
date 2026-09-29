from pyspark.sql import functions as F

lines = (
    df_app_logs
    .select(
        "id",
        "log_ts",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("attr_key", "attr_value"),
    )
)

result = (
    lines
    .groupBy(F.date_trunc("DAY", "log_ts").alias("activity_day"))
    .agg(
        F.count(F.when(F.col("attr_key") == "trace", 1)).alias("trace_rows"),
        F.sum(F.when(F.col("attr_key") == "region", F.col("status_code"))).alias("region_total_status_code"),
        ((F.unix_timestamp(F.max("log_ts")) - F.unix_timestamp(F.min("log_ts"))) / 60).alias("span_minutes"),
        F.avg("status_code").alias("avg_status_code"),
        F.percentile_approx("status_code", 0.95).alias("status_code_p95"),
        F.sum(F.when(F.col("attr_key") == "region", F.col("status_code")).otherwise(0)).alias("region_status_code"),
    )
)
