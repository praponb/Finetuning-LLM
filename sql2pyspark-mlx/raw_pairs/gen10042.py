from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("service", "level")
w2 = Window.partitionBy("attr_key")
w3 = Window.partitionBy("service", "attr_key")
w4 = Window.partitionBy("service").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-3600, Window.currentRow)
w5 = Window.partitionBy("service")

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "log_ts",
        "level",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("attr_key", "value"),
    )
)

enriched = (
    exploded
    .filter(F.col("level") == "INFO")
    .select(
        "id",
        "service",
        "log_ts",
        "level",
        "status_code",
        "attr_key",
        "value",
        F.corr("latency_ms", "status_code").over(w).alias("corr_latency_ms_status_code"),
        F.round((F.col("latency_ms") / F.sum("latency_ms").over(w2)), 4).alias("latency_ms_share"),
        F.avg("status_code").over(w3).alias("service_avg_status_code"),
        F.avg("status_code").over(w4).alias("status_code_avg_1h"),
    )
)

valid = (
    enriched
    .filter((F.col("log_ts") >= F.to_timestamp(F.lit("2025-06-01 11:00:00"))) & F.col("attr_key").isNotNull())
    .select(
        "id",
        "service",
        "status_code",
        "attr_key",
        "value",
        "corr_latency_ms_status_code",
        "latency_ms_share",
        "service_avg_status_code",
        "status_code_avg_1h",
        F.col("status_code_avg_1h").cast("string").alias("status_code_avg_1h_num"),
        F.coalesce("attr_key", F.lit("OTHER")).alias("attr_key_clean"),
    )
)

with_windows = (
    valid
    .select(
        "service",
        "attr_key",
        "corr_latency_ms_status_code",
        "attr_key_clean",
        F.round((F.col("status_code_avg_1h") / F.sum("status_code_avg_1h").over(w3)), 4).alias("status_code_avg_1h_share"),
        F.round((F.col("service_avg_status_code") / F.sum("service_avg_status_code").over(w5)), 4).alias("service_avg_status_code_share"),
        F.min("latency_ms_share").over(w5).alias("service_min_latency_ms_share"),
        F.round((F.col("status_code_avg_1h") / F.sum("status_code_avg_1h").over(w2)), 4).alias("status_code_avg_1h_share_2"),
    )
)

result = (
    with_windows
    .groupBy(
        F.when(F.col("service_avg_status_code_share") >= 0.63, "HIGH").otherwise("LOW").alias("service_avg_status_code_share_tier"),
    )
    .agg(
        F.size(F.collect_set("service")).alias("num_services"),
        F.count("*").alias("row_count"),
        F.percentile_approx("status_code_avg_1h_share_2", 0.5).alias("status_code_avg_1h_share_2_p50"),
        F.percentile_approx("service_min_latency_ms_share", [0.25, 0.5, 0.75]).alias("service_min_latency_ms_share_quartiles"),
        F.count("*").alias("event_count"),
        F.count(F.when(F.col("attr_key") == "region", 1)).alias("region_rows"),
    )
    .filter(F.col("region_rows") >= 1)
    .orderBy(F.col("num_services").desc_nulls_last())
)
