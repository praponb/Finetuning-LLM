from pyspark.sql import functions as F

lines = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("attr_key", "attr_value"),
    )
)

result = (
    lines
    .groupBy(F.date_trunc("HOUR", "log_ts").alias("hour_window"))
    .agg(
        F.round(F.avg("latency_ms"), 2).alias("avg_latency_ms"),
        F.size(F.collect_set("attr_key")).alias("num_attr_keys"),
        F.max("log_ts").alias("last_seen"),
        F.stddev_samp("latency_ms").alias("latency_ms_stddev"),
        F.sum(F.when(F.col("host") == "h3", F.col("status_code"))).alias("h3_total_status_code"),
    )
    .orderBy(F.col("num_attr_keys").desc_nulls_first())
)
