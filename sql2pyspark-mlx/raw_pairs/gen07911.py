from pyspark.sql import functions as F

flattened = (
    df_app_logs
    .select(
        "id",
        "service",
        "level",
        "status_code",
        F.explode_outer("context").alias("k", "v"),
    )
)

result = (
    flattened
    .withColumn("status_code_tier", F.when(F.col("status_code") >= 445, "PREMIUM").otherwise("STANDARD"))
    .rollup("status_code_tier", "service")
    .agg(
        F.percentile_approx("status_code", 0.95).alias("status_code_p95"),
        F.sum("status_code").alias("total_status_code"),
        F.sum(F.when(F.col("level") == "DEBUG", F.col("status_code"))).alias("debug_total_status_code"),
        F.grouping_id().alias("gid"),
    )
    .orderBy(F.col("total_status_code").asc_nulls_last())
)
