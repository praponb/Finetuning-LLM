from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("host").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-900, Window.currentRow)
w2 = Window.partitionBy(F.to_date("log_ts"), "host")
w3 = Window.partitionBy(F.month("log_ts"))
w4 = Window.partitionBy("host", "level")
w5 = Window.partitionBy("attr_key", "host").orderBy(F.col("latency_ms_share_2").asc_nulls_first())
w6 = Window.partitionBy("host", "attr_key").orderBy(F.col("latency_ms_max_15m").desc())
w7 = Window.partitionBy("attr_key").orderBy(F.col("latency_ms_share").desc_nulls_first())
w8 = Window.partitionBy("host").orderBy(F.col("host_count_latency_ms").asc_nulls_first())
w9 = Window.orderBy(F.col("latency_ms_share_2_cume_dist").desc_nulls_last())
w10 = Window.orderBy(F.col("latency_ms_share_2_cume_dist").desc())

exploded = (
    df_app_logs
    .select(
        "id",
        "host",
        "log_ts",
        "level",
        "latency_ms",
        F.explode_outer("context").alias("attr_key", "value"),
    )
)

scored = (
    exploded
    .withColumn("latency_ms_max_15m", F.max("latency_ms").over(w))
    .withColumn("latency_ms_share", F.round((F.col("latency_ms") / F.sum("latency_ms").over(w2)), 4))
)

scored_app_logs = (
    scored
    .withColumn("latency_ms_share_2", F.round((F.col("latency_ms") / F.sum("latency_ms").over(w3)), 4))
    .withColumn("host_count_latency_ms", F.count("latency_ms").over(w4))
)

metrics = (
    scored_app_logs
    .select(
        "id",
        "level",
        "attr_key",
        "latency_ms_max_15m",
        "host_count_latency_ms",
        F.cume_dist().over(w5).alias("latency_ms_share_2_cume_dist"),
        F.dense_rank().over(w6).alias("latency_ms_max_15m_dense_rank"),
        F.dense_rank().over(w7).alias("latency_ms_share_dense_rank"),
        F.percent_rank().over(w8).alias("host_count_latency_ms_percent_rank"),
    )
)

with_windows = (
    metrics
    .filter(F.col("latency_ms_share_2_cume_dist") < 0.65)
    .select(
        "id",
        "latency_ms_share_2_cume_dist",
        F.percent_rank().over(w9).alias("latency_ms_share_2_cume_dist_percent_rank"),
    )
)

metrics_app_logs = (
    with_windows
    .select(
        "id",
        "latency_ms_share_2_cume_dist",
        F.rank().over(w10).alias("latency_ms_share_2_cume_dist_rank"),
    )
)

result = (
    metrics_app_logs
    .groupBy(
        F.when(F.col("latency_ms_share_2_cume_dist") >= 0.63, "HIGH").otherwise("LOW").alias("latency_ms_share_2_cume_dist_tier"),
    )
    .agg(
        F.count("*").alias("event_count"),
        F.percentile_approx("latency_ms_share_2_cume_dist_rank", 0.95).alias("latency_ms_share_2_cume_dist_rank_p95"),
    )
)
