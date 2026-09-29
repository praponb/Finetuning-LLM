from pyspark.sql import functions as F

flattened = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("key", "v"),
    )
)

result = (
    flattened
    .withColumn("activity_day", F.date_trunc("day", "log_ts"))
    .cube("host", "activity_day")
    .agg(
        F.count(F.when(F.col("host") == "h3", 1)).alias("h3_rows"),
        F.countDistinct("id").alias("distinct_ids"),
        F.count(F.when(F.col("host") == "h2", 1)).alias("h2_rows"),
        F.avg(F.when(F.col("host") == "h1", F.col("status_code"))).alias("h1_avg_status_code"),
        F.sum(F.when(F.col("key") == "trace", F.col("latency_ms")).otherwise(0)).alias("trace_latency_ms"),
        F.grouping_id().alias("grouping_level"),
    )
    .orderBy(F.col("h1_avg_status_code").desc_nulls_first())
)
