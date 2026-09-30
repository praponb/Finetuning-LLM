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
        F.explode_outer("context").alias("k", "attr_value"),
    )
)

base = (
    flattened
    .filter(~F.col("service").isin(["search", "checkout", "billing"]))
    .withColumn("host_prefix", F.substring("host", 1, 4))
    .withColumn("k_clean", F.coalesce("k", F.lit("DIRECT")))
)

base_app_logs = (
    base
    .filter(F.col("k").isNotNull() & (F.col("service") == "search"))
    .withColumn("level_title", F.initcap(F.trim("level")))
    .withColumn("resolved_k", F.coalesce("k", F.lit("OTHER")))
)

filtered = (
    base_app_logs
    .filter(~F.col("level").isin(["INFO", "DEBUG", "ERROR"]))
    .select(
        "*",
        (F.col("status_code") - F.col("latency_ms")).alias("status_code_minus_latency_ms"),
    )
)

filtered_app_logs = (
    filtered
    .filter(F.col("k") == "region")
    .select(
        "id",
        "service",
        "host",
        "level",
        "latency_ms",
        "status_code",
        "attr_value",
        "host_prefix",
        "k_clean",
        "level_title",
        "status_code_minus_latency_ms",
        F.upper("service").alias("service_upper"),
    )
)

result = (
    filtered_app_logs
    .groupBy("host")
    .agg(
        F.count(F.when(F.col("host") == "h4", 1)).alias("h4_count"),
        F.avg("status_code").alias("avg_status_code"),
        F.stddev_samp("status_code").alias("status_code_stddev"),
        F.size(F.collect_set("service")).alias("num_services"),
        F.round(F.avg("latency_ms"), 2).alias("avg_latency_ms"),
        F.count(F.when(F.col("host") == "h2", 1)).alias("h2_rows"),
    )
)
