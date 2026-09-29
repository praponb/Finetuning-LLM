from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("host")
w2 = Window.partitionBy("host").orderBy(F.col("latency_ms_scaled").desc_nulls_last())
w3 = Window.orderBy(F.col("host_min_latency_ms_scaled").desc_nulls_last())
w4 = Window.orderBy(F.col("host_stddev_samp_latency_ms_scaled").desc_nulls_first())
w5 = Window.orderBy("host_stddev_samp_latency_ms_scaled")
w_running = Window.orderBy(F.col("host_stddev_samp_latency_ms_scaled_cume_dist_2").desc_nulls_first())
w7 = Window.orderBy(F.col("host_stddev_samp_latency_ms_scaled").desc_nulls_last())
w_hist = Window.orderBy(F.col("host_stddev_samp_latency_ms_scaled_dense_rank").desc_nulls_first())
w9 = Window.orderBy("host_stddev_samp_latency_ms_scaled_cume_dist")
w10 = Window.orderBy(F.col("host_stddev_samp_latency_ms_scaled_cume_dist_2_rank").desc_nulls_last())

lines = (
    df_app_logs
    .select(
        "id",
        "host",
        "latency_ms",
        F.explode_outer("context").alias("attr_key", "v"),
    )
)

base = (
    lines
    .filter(F.col("attr_key").isNotNull())
    .select(
        "id",
        "host",
        "attr_key",
        "v",
        F.when(F.col("host").isin(["h4", "h3"]), "Priority").otherwise("Other").alias("host_group"),
        F.round((F.col("latency_ms") * 100), 2).alias("latency_ms_scaled"),
    )
)

base_app_logs = (
    base
    .filter((~F.col("attr_key").eqNullSafe(F.lit("region"))) & (F.col("latency_ms_scaled") >= 71))
    .select(
        "id",
        "host",
        "v",
        "host_group",
        "latency_ms_scaled",
        F.upper("attr_key").alias("attr_key_upper"),
        F.upper("host").alias("host_upper"),
    )
)

with_windows = (
    base_app_logs
    .select(
        "id",
        "attr_key_upper",
        "host_upper",
        F.min("latency_ms_scaled").over(w).alias("host_min_latency_ms_scaled"),
        F.stddev_samp("latency_ms_scaled").over(w).alias("host_stddev_samp_latency_ms_scaled"),
        F.dense_rank().over(w2).alias("latency_ms_scaled_dense_rank"),
    )
)

metrics = (
    with_windows
    .select(
        "*",
        F.rank().over(w3).alias("host_min_latency_ms_scaled_rank"),
        F.cume_dist().over(w4).alias("host_stddev_samp_latency_ms_scaled_cume_dist"),
        F.cume_dist().over(w5).alias("host_stddev_samp_latency_ms_scaled_cume_dist_2"),
    )
)

enriched = (
    metrics
    .select(
        "attr_key_upper",
        "latency_ms_scaled_dense_rank",
        "host_stddev_samp_latency_ms_scaled_cume_dist",
        "host_stddev_samp_latency_ms_scaled_cume_dist_2",
        F.dense_rank().over(w_running).alias("host_stddev_samp_latency_ms_scaled_cume_dist_2_dense_rank"),
        F.rank().over(w7).alias("host_stddev_samp_latency_ms_scaled_rank"),
        F.dense_rank().over(w5).alias("host_stddev_samp_latency_ms_scaled_dense_rank"),
        F.rank().over(w_running).alias("host_stddev_samp_latency_ms_scaled_cume_dist_2_rank"),
    )
)

result = (
    enriched
    .select(
        "host_stddev_samp_latency_ms_scaled_cume_dist_2_dense_rank",
        "host_stddev_samp_latency_ms_scaled_rank",
        "host_stddev_samp_latency_ms_scaled_cume_dist_2_rank",
        F.cume_dist().over(w_hist).alias("host_stddev_samp_latency_ms_scaled_dense_rank_cume_dist"),
        F.rank().over(w9).alias("host_stddev_samp_latency_ms_scaled_cume_dist_rank"),
        F.rank().over(w10).alias("host_stddev_samp_latency_ms_scaled_cume_dist_2_rank_rank"),
    )
)
