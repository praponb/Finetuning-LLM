from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("k").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-1800, Window.currentRow)
w2 = Window.partitionBy("k").orderBy(F.col("latency_ms").desc())
w3 = Window.partitionBy("k", "level").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-900, Window.currentRow)
w4 = Window.partitionBy("k", "service").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-3600, Window.currentRow)
w_ordered = Window.orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-7200, Window.currentRow)
w6 = Window.partitionBy("service")
w7 = Window.partitionBy("k", "service").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-1800, Window.currentRow)
w8 = Window.partitionBy("k")
w9 = Window.partitionBy("level", "host")
w10 = Window.partitionBy("service", "k").orderBy(F.col("latency_ms_max_30m_max_2h").desc_nulls_first())
w11 = Window.partitionBy("service").orderBy(F.col("latency_ms_share").desc_nulls_last())
w12 = Window.orderBy("latency_ms_share")
w_running = Window.partitionBy("service", "k")

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "level",
        "latency_ms",
        F.explode_outer("context").alias("k", "v"),
    )
)

metrics = (
    exploded
    .withColumn("latency_ms_max_30m", F.max("latency_ms").over(w))
    .withColumn("latency_ms_percent_rank", F.percent_rank().over(w2))
)

scored = (
    metrics
    .withColumn("latency_ms_avg_15m", F.avg("latency_ms").over(w3))
    .withColumn("latency_ms_max_30m_max_1h", F.max("latency_ms_max_30m").over(w4))
    .withColumn("latency_ms_max_30m_max_2h", F.max("latency_ms_max_30m").over(w_ordered))
    .withColumn("latency_ms_share", F.round((F.col("latency_ms") / F.sum("latency_ms").over(w6)), 4))
)

enriched = (
    scored
    .select(
        "*",
        F.avg("latency_ms").over(w7).alias("latency_ms_avg_30m"),
        F.round((F.col("latency_ms") / F.sum("latency_ms").over(w8)), 4).alias("latency_ms_share_2"),
        F.stddev_samp("latency_ms_share").over(w9).alias("level_stddev_samp_latency_ms_share"),
        F.percent_rank().over(w10).alias("latency_ms_max_30m_max_2h_percent_rank"),
    )
)

cleaned = (
    enriched
    .filter(F.col("k").isNotNull())
    .select(
        "service",
        "log_ts",
        "latency_ms",
        "k",
        "v",
        "latency_ms_avg_15m",
        "latency_ms_max_30m_max_1h",
        "latency_ms_max_30m_max_2h",
        "latency_ms_share",
        "latency_ms_avg_30m",
        "level_stddev_samp_latency_ms_share",
        "latency_ms_max_30m_max_2h_percent_rank",
        F.coalesce("latency_ms_share_2", F.lit(0)).alias("latency_ms_share_2_filled"),
    )
)

enriched_app_logs = (
    cleaned
    .withColumn("latency_ms_share_cume_dist", F.cume_dist().over(w11))
    .withColumn("k_stddev_samp_latency_ms_max_30m_max_2h_percent_rank", F.stddev_samp("latency_ms_max_30m_max_2h_percent_rank").over(w8))
    .withColumn("latency_ms_share_rank", F.rank().over(w12))
    .withColumn("service_max_latency_ms_avg_15m", F.max("latency_ms_avg_15m").over(w_running))
)

result = (
    enriched_app_logs
    .groupBy(F.date_trunc("DAY", "log_ts").alias("activity_day"))
    .agg(
        F.sort_array(F.collect_set("service")).alias("service_list"),
        F.sort_array(F.collect_set("k")).alias("k_list"),
    )
)
