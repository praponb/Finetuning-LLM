from pyspark.sql import functions as F

exploded = (
    df_app_logs
    .select(
        "id",
        "log_ts",
        "latency_ms",
        "status_code",
        F.explode("context").alias("key", "v"),
    )
)

result = (
    exploded
    .withColumn("hour_window", F.date_trunc("HOUR", "log_ts"))
    .groupingSets([["hour_window", "key"], ["hour_window"], []], "hour_window", "key")
    .agg(
        F.sum(F.when(F.col("key") == "region", F.col("status_code")).otherwise(0)).alias("region_status_code"),
        F.corr("latency_ms", "status_code").alias("corr_latency_ms_status_code"),
        F.avg(F.when(F.col("key") == "version", F.col("status_code"))).alias("version_avg_status_code"),
        F.sum(F.when(F.col("key") == "region", F.col("latency_ms")).otherwise(0)).alias("region_latency_ms"),
        F.grouping_id().alias("gid"),
    )
)
