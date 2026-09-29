from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("level")
w2 = Window.partitionBy("service", "level").orderBy(F.col("latency_ms").desc())
w3 = Window.partitionBy("level", "service")
w4 = Window.orderBy(F.col("level_count_latency_ms").desc())
w5 = Window.partitionBy("level", "key").orderBy(F.col("latency_ms_share").desc_nulls_first())
w6 = Window.partitionBy("key").orderBy(F.col("latency_ms_percent_rank").asc_nulls_first())
w7 = Window.partitionBy("key").orderBy(F.col("level_max_latency_ms").desc_nulls_first())
w8 = Window.partitionBy("level").orderBy(F.col("level_max_latency_ms_whole").desc_nulls_last())
w9 = Window.partitionBy("key", "level").orderBy(F.col("latency_ms_percent_rank_rank").asc_nulls_first())

lines = (
    df_app_logs
    .select(
        "id",
        "service",
        "level",
        "latency_ms",
        F.explode_outer("context").alias("key", "v"),
    )
)

metrics = (
    lines
    .select(
        "*",
        F.max("latency_ms").over(w_part).alias("level_max_latency_ms"),
        F.count("latency_ms").over(w_part).alias("level_count_latency_ms"),
        F.percent_rank().over(w2).alias("latency_ms_percent_rank"),
        F.round((F.col("latency_ms") / F.sum("latency_ms").over(w3)), 4).alias("latency_ms_share"),
    )
)

cleaned = (
    metrics
    .filter(F.col("key").isNotNull() & F.col("level_count_latency_ms").between(8, 15))
    .select(
        "id",
        "level",
        "key",
        "v",
        "level_max_latency_ms",
        "level_count_latency_ms",
        "latency_ms_percent_rank",
        "latency_ms_share",
        F.floor("level_max_latency_ms").alias("level_max_latency_ms_whole"),
        F.coalesce("key", F.lit("OTHER")).alias("key_clean"),
    )
)

with_windows = (
    cleaned
    .select(
        "*",
        F.rank().over(w4).alias("level_count_latency_ms_rank"),
        F.cume_dist().over(w5).alias("latency_ms_share_cume_dist"),
        F.rank().over(w6).alias("latency_ms_percent_rank_rank"),
    )
)

with_windows_app_logs = (
    with_windows
    .withColumn("level_min_level_count_latency_ms_rank", F.min("level_count_latency_ms_rank").over(w_part))
    .withColumn("level_max_latency_ms_rank", F.rank().over(w7))
)

metrics_app_logs = (
    with_windows_app_logs
    .select(
        "*",
        F.percent_rank().over(w8).alias("level_max_latency_ms_whole_percent_rank"),
        F.percent_rank().over(w9).alias("latency_ms_percent_rank_rank_percent_rank"),
    )
)

result = (
    metrics_app_logs
    .groupBy(
        F.when(F.col("level_max_latency_ms") >= 1918, "HIGH").otherwise("LOW").alias("level_max_latency_ms_tier"),
        "key",
    )
    .agg(
        F.percentile_approx("level_min_level_count_latency_ms_rank", [0.25, 0.5, 0.75]).alias("level_min_level_count_latency_ms_rank_quartiles"),
        F.sum("level_max_latency_ms").alias("total_level_max_latency_ms"),
    )
    .orderBy("total_level_max_latency_ms")
)
