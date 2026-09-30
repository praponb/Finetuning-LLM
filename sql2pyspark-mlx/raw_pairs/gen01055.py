from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("key")
w2 = Window.partitionBy("level")
w_running = Window.partitionBy("level", "key")
w4 = Window.partitionBy("level").orderBy(F.col("status_code").desc())

flattened = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "level",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("key", "v"),
    )
)

cleaned = (
    flattened
    .filter(F.col("key").isNotNull())
    .select(
        "id",
        "service",
        "level",
        "latency_ms",
        "status_code",
        "key",
        "v",
        F.concat_ws("-", "key", "level").alias("key_level_key"),
        F.coalesce("key", F.lit("UNKNOWN")).alias("key_clean"),
    )
)

enriched = (
    cleaned
    .select(
        "level",
        "latency_ms",
        "v",
        "key_clean",
        F.round((F.col("latency_ms") / F.sum("latency_ms").over(w)), 4).alias("latency_ms_share"),
        F.stddev_samp("latency_ms").over(w2).alias("level_stddev_samp_latency_ms"),
        F.corr("status_code", "latency_ms").over(w_running).alias("corr_status_code_latency_ms"),
        F.percent_rank().over(w4).alias("status_code_percent_rank"),
        F.round((F.col("latency_ms") / F.sum("latency_ms").over(w)), 4).alias("latency_ms_share_2"),
    )
)

cleaned_app_logs = (
    enriched
    .filter(F.col("corr_status_code_latency_ms") < 0.5)
    .select(
        "*",
        (F.col("status_code_percent_rank") - F.col("latency_ms_share")).alias("status_code_percent_rank_minus_latency_ms_share"),
        F.round("latency_ms_share_2", 1).alias("latency_ms_share_2_rounded"),
    )
)

valid = (
    cleaned_app_logs
    .filter(F.col("latency_ms_share_2_rounded") < 33)
    .select(
        "*",
        F.when(F.col("level_stddev_samp_latency_ms") >= 2053, "High").when(F.col("level_stddev_samp_latency_ms") >= 882, "Medium").otherwise("Low").alias("level_stddev_samp_latency_ms_tier"),
    )
)

summary = (
    valid
    .groupBy("level")
    .agg(
        F.sort_array(F.collect_set("level")).alias("level_list"),
        F.percentile_approx("latency_ms_share_2_rounded", [0.25, 0.5, 0.75]).alias("latency_ms_share_2_rounded_quartiles"),
    )
)

result = summary.select("level", "latency_ms_share_2_rounded_quartiles")
