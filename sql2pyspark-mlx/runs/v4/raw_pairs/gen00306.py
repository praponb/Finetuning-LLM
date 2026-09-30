from pyspark.sql import functions as F

lines = (
    df_app_logs
    .select(
        "id",
        "service",
        "log_ts",
        "level",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("k", "value"),
    )
)

result = (
    lines
    .withColumn("status_code_tier", F.when(F.col("status_code") >= 420, "PREMIUM").otherwise("STANDARD"))
    .rollup("level", "status_code_tier")
    .agg(
        F.count("*").alias("row_count"),
        F.max("log_ts").alias("last_seen"),
        F.count(F.when(F.col("level") == "DEBUG", 1)).alias("debug_rows"),
        F.grouping_id().alias("grouping_id"),
    )
)
