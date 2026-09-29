from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-1800, Window.currentRow)
w_part = Window.partitionBy("host").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-1800, Window.currentRow)
w3 = Window.partitionBy("k", "host").orderBy(F.col("latency_ms").asc_nulls_last())
w4 = Window.partitionBy(F.date_trunc("YEAR", "log_ts"), "k")
w5 = Window.partitionBy(F.date_trunc("MONTH", "log_ts"), "k")
w6 = Window.partitionBy("k")
w7 = Window.partitionBy("host").orderBy("status_code")
w_running = Window.partitionBy("host", "k").orderBy(F.col("status_code_max_30m_2").asc_nulls_first())
w9 = Window.partitionBy("k").orderBy(F.col("status_code_max_30m_2_avg_30m").desc_nulls_last())
w10 = Window.orderBy(F.col("status_code_max_30m_2_avg_30m").desc_nulls_last())
w11 = Window.partitionBy("k").orderBy(F.col("corr_latency_ms_status_code").desc_nulls_last())

exploded = (
    df_app_logs
    .select(
        "id",
        "host",
        "log_ts",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("k", "attr_value"),
    )
)

metrics = (
    exploded
    .withColumn("status_code_max_30m", F.max("status_code").over(w_grp))
    .withColumn("status_code_max_30m_2", F.max("status_code").over(w_part))
    .withColumn("latency_ms_cume_dist", F.cume_dist().over(w3))
    .withColumn("latency_ms_share", F.round((F.col("latency_ms") / F.sum("latency_ms").over(w4)), 4))
    .withColumn("latency_ms_share_2", F.round((F.col("latency_ms") / F.sum("latency_ms").over(w5)), 4))
)

scored = (
    metrics
    .select(
        "id",
        "latency_ms",
        "status_code",
        "k",
        "attr_value",
        "status_code_max_30m",
        "status_code_max_30m_2",
        F.corr("latency_ms", "status_code").over(w6).alias("corr_latency_ms_status_code"),
        F.dense_rank().over(w7).alias("status_code_dense_rank"),
        F.percent_rank().over(w_running).alias("status_code_max_30m_2_percent_rank"),
        F.avg("status_code_max_30m_2").over(w_grp).alias("status_code_max_30m_2_avg_30m"),
    )
)

with_windows = (
    scored
    .select(
        "*",
        F.sum("status_code_dense_rank").over(w6).alias("k_sum_status_code_dense_rank"),
        F.round((F.col("status_code_max_30m_2_avg_30m") / F.sum("status_code_max_30m_2_avg_30m").over(w6)), 4).alias("status_code_max_30m_2_avg_30m_share"),
        F.rank().over(w9).alias("status_code_max_30m_2_avg_30m_rank"),
    )
)

metrics_app_logs = (
    with_windows
    .filter(F.col("status_code_max_30m_2_avg_30m_share") >= 0.39)
    .select(
        "latency_ms",
        "status_code",
        "k",
        "attr_value",
        "status_code_max_30m",
        "status_code_dense_rank",
        "status_code_max_30m_2_avg_30m",
        "status_code_max_30m_2_avg_30m_share",
        "status_code_max_30m_2_avg_30m_rank",
        F.percent_rank().over(w10).alias("status_code_max_30m_2_avg_30m_percent_rank"),
        F.round((F.col("status_code_dense_rank") / F.sum("status_code_dense_rank").over(w6)), 4).alias("status_code_dense_rank_share"),
        F.dense_rank().over(w11).alias("corr_latency_ms_status_code_dense_rank"),
    )
)

cleaned = (
    metrics_app_logs
    .filter(F.col("k").isNotNull())
    .select(
        "latency_ms",
        "k",
        "attr_value",
        "status_code_max_30m",
        "status_code_dense_rank",
        "status_code_max_30m_2_avg_30m_rank",
        "status_code_dense_rank_share",
        "corr_latency_ms_status_code_dense_rank",
        F.ceil("status_code_dense_rank_share").alias("status_code_dense_rank_share_whole"),
    )
)

result = (
    cleaned
    .select(
        "latency_ms",
        "k",
        "status_code_max_30m_2_avg_30m_rank",
        "status_code_dense_rank_share",
        "status_code_dense_rank_share_whole",
    )
    .orderBy(F.col("status_code_max_30m_2_avg_30m_rank").asc_nulls_last())
)
