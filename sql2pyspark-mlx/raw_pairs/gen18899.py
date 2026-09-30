from pyspark.sql import functions as F

lines = (
    df_app_logs
    .select(
        "id",
        "host",
        "log_ts",
        "latency_ms",
        F.explode_outer("context").alias("k", "attr_value"),
    )
)

result = (
    lines
    .withColumn("latency_ms_tier", F.when(F.col("latency_ms") >= 2249, "HIGH").otherwise("LOW"))
    .groupingSets(
        [["latency_ms_tier", "k"], ["latency_ms_tier"], []],
        "latency_ms_tier",
        "k",
    )
    .agg(
        F.sort_array(F.collect_set("k")).alias("k_list"),
        F.round(F.avg("latency_ms"), 2).alias("avg_latency_ms"),
        F.count(F.when(F.col("host") == "h2", 1)).alias("h2_rows"),
        ((F.unix_timestamp(F.max("log_ts")) - F.unix_timestamp(F.min("log_ts"))) / 60).alias("span_minutes"),
        F.grouping_id().alias("gid"),
    )
)
