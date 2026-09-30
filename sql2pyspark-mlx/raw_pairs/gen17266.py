from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level").orderBy(F.col("latency_ms").asc_nulls_last())
w2 = Window.partitionBy("service")
w3 = Window.partitionBy("attr_key")
w4 = Window.orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-3600, Window.currentRow)
w5 = Window.partitionBy("host", "service")
w6 = Window.orderBy(F.col("latency_ms_dense_rank").desc())
w7 = Window.orderBy(F.col("latency_ms_share").desc_nulls_first())
w8 = Window.partitionBy("level", "service")
w9 = Window.partitionBy("service", "level")

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "level",
        "latency_ms",
        F.explode_outer("context").alias("attr_key", "attr_value"),
    )
)

enriched = (
    exploded
    .filter(F.col("level").isin(["ERROR", "INFO", "DEBUG"]))
    .select(
        "*",
        F.dense_rank().over(w).alias("latency_ms_dense_rank"),
        F.round((F.col("latency_ms") / F.sum("latency_ms").over(w2)), 4).alias("latency_ms_share"),
    )
)

with_windows = (
    enriched
    .select(
        "id",
        "service",
        "level",
        "latency_ms",
        "latency_ms_dense_rank",
        "latency_ms_share",
        F.count("latency_ms_share").over(w3).alias("attr_key_count_latency_ms_share"),
        F.avg("latency_ms_dense_rank").over(w4).alias("latency_ms_dense_rank_avg_1h"),
        F.count("latency_ms_share").over(w5).alias("host_count_latency_ms_share"),
    )
)

enriched_app_logs = (
    with_windows
    .withColumn("latency_ms_dense_rank_rank", F.rank().over(w6))
    .withColumn("latency_ms_share_cume_dist", F.cume_dist().over(w7))
    .withColumn("latency_ms_dense_rank_avg_1h_share", F.round((F.col("latency_ms_dense_rank_avg_1h") / F.sum("latency_ms_dense_rank_avg_1h").over(w8)), 4))
    .withColumn("service_stddev_samp_attr_key_count_latency_ms_share", F.stddev_samp("attr_key_count_latency_ms_share").over(w2))
    .withColumn("latency_ms_share_2", F.round((F.col("latency_ms") / F.sum("latency_ms").over(w9)), 4))
)

remaining_keys = (
    enriched_app_logs
    .select("service")
    .intersect(
        df_app_logs
        .filter(F.col("level") == "INFO")
        .select("service")
    )
)

verified = (
    enriched_app_logs
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.service") == F.col("k.service")), "inner")
    .select("s.*")
)

remaining_keys_2 = (
    verified
    .select("id", "service")
    .exceptAll(
        df_app_logs
        .filter(F.col("host").isin(["h2", "h4", "h3"]))
        .select("id", "service")
    )
)

reconciled = (
    verified
    .alias("s")
    .join(remaining_keys_2.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.service") == F.col("k.service")), "inner")
    .select("s.*")
)

valid = (
    reconciled
    .filter(F.col("latency_ms_share_2") >= 0.21)
    .select(
        "service",
        "level",
        "latency_ms_dense_rank",
        "attr_key_count_latency_ms_share",
        "latency_ms_dense_rank_avg_1h",
        "host_count_latency_ms_share",
        "latency_ms_dense_rank_rank",
        "latency_ms_share_cume_dist",
        "latency_ms_dense_rank_avg_1h_share",
        "service_stddev_samp_attr_key_count_latency_ms_share",
        "latency_ms_share_2",
        F.coalesce("latency_ms_dense_rank_rank", F.lit(0)).alias("latency_ms_dense_rank_rank_filled"),
    )
)

result = (
    valid
    .select(
        "service",
        "level",
        "latency_ms_dense_rank",
        "attr_key_count_latency_ms_share",
        "latency_ms_dense_rank_avg_1h",
        "host_count_latency_ms_share",
        "latency_ms_dense_rank_rank",
        "latency_ms_share_cume_dist",
        "latency_ms_share_2",
    )
)
