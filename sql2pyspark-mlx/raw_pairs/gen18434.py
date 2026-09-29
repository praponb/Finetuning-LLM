from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("service", "level").orderBy(F.col("latency_ms").asc_nulls_first())
w2 = Window.partitionBy("host").orderBy(F.col("latency_ms").desc())
w3 = Window.orderBy(F.col("latency_ms").desc())
w4 = Window.partitionBy("key")
w5 = Window.partitionBy("host", "level").orderBy(F.col("latency_ms_cume_dist_2").desc())
w6 = Window.partitionBy("host").orderBy(F.col("latency_ms_cume_dist").desc())
w_ordered = Window.partitionBy("key").orderBy(F.col("latency_ms_share").desc_nulls_last())
w8 = Window.partitionBy("service")
w_hist = Window.partitionBy("host")
w10 = Window.partitionBy("level").orderBy(F.col("billing_host_count_latency_ms_cume_dist_2_dense_rank").desc())

lines = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "level",
        "latency_ms",
        F.explode_outer("context").alias("key", "v"),
    )
)

matched_keys = (
    lines
    .select("id", "level")
    .exceptAll(
        df_app_logs
        .filter(F.col("latency_ms") > 2010)
        .select("id", "level")
    )
)

kept_rows = (
    lines
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.level") == F.col("k.level")), "inner")
    .select("s.*")
)

with_windows = (
    kept_rows
    .filter(F.col("level") != "DEBUG")
    .select(
        "*",
        F.dense_rank().over(w).alias("latency_ms_dense_rank"),
        F.cume_dist().over(w2).alias("latency_ms_cume_dist"),
        F.cume_dist().over(w3).alias("latency_ms_cume_dist_2"),
        F.round((F.col("latency_ms") / F.sum("latency_ms").over(w4)), 4).alias("latency_ms_share"),
    )
)

enriched = (
    with_windows
    .withColumn("key_min_latency_ms", F.min("latency_ms").over(w4))
    .withColumn("latency_ms_cume_dist_2_dense_rank", F.dense_rank().over(w5))
    .withColumn("latency_ms_cume_dist_percent_rank", F.percent_rank().over(w6))
)

metrics = (
    enriched
    .select(
        "*",
        F.dense_rank().over(w_ordered).alias("latency_ms_share_dense_rank"),
        F.round((F.col("latency_ms_dense_rank") / F.sum("latency_ms_dense_rank").over(w8)), 4).alias("latency_ms_dense_rank_share"),
        F.count("latency_ms_cume_dist_2_dense_rank").over(w_hist).alias("host_count_latency_ms_cume_dist_2_dense_rank"),
    )
)

result = (
    metrics
    .groupBy("level", "key")
    .agg(
        F.count(F.when(F.col("service") == "auth", 1)).alias("auth_rows"),
        F.sort_array(F.collect_set("key")).alias("key_list"),
        F.count(F.when(F.col("service") == "billing", 1)).alias("billing_count"),
        F.sum(F.when(F.col("service") == "billing", F.col("host_count_latency_ms_cume_dist_2_dense_rank")).otherwise(0)).alias("billing_host_count_latency_ms_cume_dist_2_dense_rank"),
    )
    .filter(F.col("billing_count") > 1)
    .withColumn("billing_host_count_latency_ms_cume_dist_2_dense_rank_rank", F.rank().over(w10))
    .orderBy(F.col("level").desc_nulls_first())
)
