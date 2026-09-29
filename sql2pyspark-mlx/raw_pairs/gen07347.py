from pyspark.sql import functions as F

flattened_attrs = (
    df_app_logs
    .filter(F.col("host") == "h4")
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "level",
        "latency_ms",
        "status_code",
        "payload_json",
        F.shiftright("status_code", 2).alias("status_code_high"),
        F.col("context").getItem("version").alias("version"),
    )
)

result = (
    flattened_attrs
    .groupBy(
        F.when(F.col("status_code_high") >= 2, "PREMIUM").otherwise("STANDARD").alias("status_code_high_tier"),
    )
    .agg(
        F.count(F.when(F.col("level") == "ERROR", 1)).alias("error_count"),
        F.min("log_ts").alias("started_at"),
        F.count("*").alias("cnt"),
        F.countDistinct("version").alias("distinct_versions"),
        F.percentile_approx("latency_ms", [0.25, 0.5, 0.75]).alias("latency_ms_quartiles"),
    )
    .orderBy(F.col("distinct_versions").asc_nulls_first())
)
