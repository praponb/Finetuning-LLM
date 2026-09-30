from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("attr_key")
w2 = Window.orderBy("latency_ms_share")
w3 = Window.orderBy(F.col("attr_key_min_latency_ms").desc_nulls_last())
w4 = Window.orderBy(F.col("attr_key_stddev_samp_latency_ms").asc_nulls_first())
w5 = Window.orderBy(F.col("latency_ms_share_percent_rank").desc())
w6 = Window.orderBy(F.col("latency_ms_share").desc_nulls_first())
w7 = Window.orderBy(F.col("latency_ms_share_percent_rank").asc_nulls_first())
w8 = Window.orderBy(F.col("latency_ms_share").desc())

flattened = (
    df_app_logs
    .select(
        "id",
        "latency_ms",
        F.explode_outer("context").alias("attr_key", "attr_value"),
    )
)

metrics = (
    flattened
    .select(
        "attr_key",
        "attr_value",
        F.min("latency_ms").over(w).alias("attr_key_min_latency_ms"),
        F.round((F.col("latency_ms") / F.sum("latency_ms").over(w)), 4).alias("latency_ms_share"),
        F.stddev_samp("latency_ms").over(w).alias("attr_key_stddev_samp_latency_ms"),
        F.min("latency_ms").over(w).alias("attr_key_min_latency_ms_2"),
        F.round((F.col("latency_ms") / F.sum("latency_ms").over(w)), 4).alias("latency_ms_share_2"),
    )
)

valid = (
    metrics
    .filter(F.col("attr_key").isNotNull())
    .select(
        "*",
        F.initcap(F.trim("attr_key")).alias("attr_key_title"),
        F.initcap(F.trim("attr_key")).alias("attr_key_title_2"),
        F.coalesce("attr_key", F.lit("OTHER")).alias("attr_key_clean"),
    )
)

with_windows = (
    valid
    .select(
        "attr_value",
        "latency_ms_share",
        "attr_key_stddev_samp_latency_ms",
        "latency_ms_share_2",
        "attr_key_title",
        F.percent_rank().over(w2).alias("latency_ms_share_percent_rank"),
        F.cume_dist().over(w3).alias("attr_key_min_latency_ms_cume_dist"),
    )
)

with_windows_app_logs = (
    with_windows
    .select(
        "attr_value",
        "latency_ms_share",
        "attr_key_title",
        "latency_ms_share_percent_rank",
        F.dense_rank().over(w4).alias("attr_key_stddev_samp_latency_ms_dense_rank"),
        F.percent_rank().over(w5).alias("latency_ms_share_percent_rank_percent_rank"),
        F.cume_dist().over(w6).alias("latency_ms_share_cume_dist"),
        F.dense_rank().over(w7).alias("latency_ms_share_percent_rank_dense_rank"),
        F.percent_rank().over(w8).alias("latency_ms_share_percent_rank_2"),
    )
)

result = (
    with_windows_app_logs
    .groupBy(
        F.when(F.col("attr_key_stddev_samp_latency_ms_dense_rank") >= 7, "LARGE").otherwise("SMALL").alias("attr_key_stddev_samp_latency_ms_dense_rank_tier"),
        F.when(F.col("latency_ms_share_percent_rank") >= 0.89, "PREMIUM").otherwise("STANDARD").alias("latency_ms_share_percent_rank_tier"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.min("latency_ms_share_percent_rank_2").alias("min_latency_ms_share_percent_rank_2"),
        F.count("*").alias("row_count"),
    )
    .orderBy(F.col("cnt").asc_nulls_last())
)
