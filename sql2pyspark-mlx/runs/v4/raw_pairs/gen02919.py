from pyspark.sql import functions as F

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "log_ts",
        "level",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("attr_key", "attr_value"),
    )
)

result = (
    exploded
    .withColumn("status_code_tier", F.when(F.col("status_code") >= 460, "LARGE").otherwise("SMALL"))
    .withColumn("latency_ms_tier", F.when(F.col("latency_ms") >= 2679, "HIGH").otherwise("LOW"))
    .groupingSets(
        [["status_code_tier", "level", "latency_ms_tier"], ["status_code_tier", "level"]],
        "status_code_tier",
        "level",
        "latency_ms_tier",
    )
    .agg(
        F.round(F.avg("status_code"), 2).alias("avg_status_code"),
        F.sum(F.when(F.col("level") == "DEBUG", F.col("status_code")).otherwise(0)).alias("debug_status_code"),
        ((F.unix_timestamp(F.max("log_ts")) - F.unix_timestamp(F.min("log_ts"))) / 60).alias("span_minutes"),
        F.size(F.collect_set("level")).alias("num_levels"),
        F.percentile_approx("latency_ms", 0.9).alias("latency_ms_p90"),
    )
)
