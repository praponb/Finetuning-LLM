from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("latency_ms").desc())
w2 = Window.orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-900, Window.currentRow)
w3 = Window.partitionBy("k")
w4 = Window.orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-7200, Window.currentRow)
w5 = Window.partitionBy("level").orderBy(F.col("latency_ms_dense_rank").desc_nulls_first())
w6 = Window.partitionBy("level")
w7 = Window.partitionBy("level").orderBy("latency_ms_dense_rank")
w8 = Window.partitionBy("level").orderBy(F.col("k_count_latency_ms_max_2h").asc_nulls_first())
w9 = Window.orderBy(F.col("latency_ms_dense_rank").desc_nulls_last())
w10 = Window.orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-1800, Window.currentRow)
w11 = Window.partitionBy("level").orderBy("k_count_latency_ms_max_2h_percent_rank")
w12 = Window.orderBy(F.col("latency_ms_dense_rank_dense_rank").asc_nulls_last())
w_ordered = Window.partitionBy("level").orderBy(F.col("latency_ms_dense_rank_sum_30m").desc_nulls_first())

exploded = (
    df_app_logs
    .select(
        "id",
        "log_ts",
        "level",
        "latency_ms",
        F.explode_outer("context").alias("k", "v"),
    )
)

metrics = (
    exploded
    .filter(F.col("k") != "version")
    .select(
        "log_ts",
        "level",
        "v",
        F.dense_rank().over(w).alias("latency_ms_dense_rank"),
        F.avg("latency_ms").over(w2).alias("latency_ms_avg_15m"),
        F.count("latency_ms").over(w3).alias("k_count_latency_ms"),
    )
)

scored = (
    metrics
    .select(
        "log_ts",
        "level",
        "v",
        "latency_ms_dense_rank",
        F.max("k_count_latency_ms").over(w4).alias("k_count_latency_ms_max_2h"),
        F.cume_dist().over(w5).alias("latency_ms_dense_rank_cume_dist"),
        F.round((F.col("latency_ms_dense_rank") / F.sum("latency_ms_dense_rank").over(w6)), 4).alias("latency_ms_dense_rank_share"),
        F.round((F.col("latency_ms_avg_15m") / F.sum("latency_ms_avg_15m").over(w6)), 4).alias("latency_ms_avg_15m_share"),
    )
)

with_windows = (
    scored
    .select(
        "level",
        "latency_ms_avg_15m_share",
        F.round((F.col("latency_ms_dense_rank") / F.sum("latency_ms_dense_rank").over(w6)), 4).alias("latency_ms_dense_rank_share_2"),
        F.cume_dist().over(w7).alias("latency_ms_dense_rank_cume_dist_2"),
        F.percent_rank().over(w8).alias("k_count_latency_ms_max_2h_percent_rank"),
        F.dense_rank().over(w9).alias("latency_ms_dense_rank_dense_rank"),
        F.sum("latency_ms_dense_rank").over(w10).alias("latency_ms_dense_rank_sum_30m"),
    )
)

result = (
    with_windows
    .select(
        "latency_ms_avg_15m_share",
        "latency_ms_dense_rank_cume_dist_2",
        F.cume_dist().over(w11).alias("k_count_latency_ms_max_2h_percent_rank_cume_dist"),
        F.count("latency_ms_dense_rank_share_2").over(w6).alias("level_count_latency_ms_dense_rank_share_2"),
        F.percent_rank().over(w12).alias("latency_ms_dense_rank_dense_rank_percent_rank"),
        F.rank().over(w_ordered).alias("latency_ms_dense_rank_sum_30m_rank"),
    )
)
