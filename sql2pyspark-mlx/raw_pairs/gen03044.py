from pyspark.sql import functions as F

flattened = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "level",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("key", "value"),
    )
)

result = (
    flattened
    .groupBy(
        "key",
        "level",
        F.when(F.col("latency_ms") >= 2017, "PREMIUM").otherwise("STANDARD").alias("latency_ms_tier"),
    )
    .agg(
        F.count(F.when(F.col("level") == "ERROR", 1)).alias("error_rows"),
        F.sum(F.when(F.col("key") == "version", F.col("status_code"))).alias("version_total_status_code"),
        F.count(F.when(F.col("level") == "ERROR", 1)).alias("error_count"),
        F.sum(F.when(F.col("level") == "ERROR", F.col("status_code")).otherwise(0)).alias("error_status_code"),
        F.sort_array(F.collect_set("level")).alias("level_list"),
        F.sum(F.when(F.col("level") == "INFO", F.col("status_code")).otherwise(0)).alias("info_status_code"),
    )
)
