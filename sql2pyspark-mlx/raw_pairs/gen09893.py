from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("level")
w2 = Window.orderBy("k_length")
w3 = Window.partitionBy("k").orderBy(F.col("k_length").desc_nulls_last())
w4 = Window.partitionBy("level", "k").orderBy(F.col("k_length").asc_nulls_last())
w5 = Window.orderBy("level_stddev_samp_k_length")

lines = (
    df_app_logs
    .select(
        "id",
        "log_ts",
        "level",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("k", "v"),
    )
)

base = (
    lines
    .filter((F.col("log_ts") >= F.to_date(F.lit("2025-06-01"))) & F.col("k").isNotNull())
    .select(
        "id",
        "log_ts",
        "level",
        "latency_ms",
        "status_code",
        "k",
        "v",
        F.length("k").alias("k_length"),
        F.round("latency_ms", 0).alias("latency_ms_rounded"),
        F.coalesce("k", F.lit("OTHER")).alias("k_clean"),
    )
)

base_app_logs = (
    base
    .filter(F.col("k").isNotNull())
    .select(
        "id",
        "log_ts",
        "level",
        "status_code",
        "k",
        "k_length",
        "k_clean",
        F.coalesce("k", F.lit("unassigned")).alias("k_filled"),
        F.coalesce("k", F.lit("UNKNOWN")).alias("k_clean_2"),
    )
)

metrics = (
    base_app_logs
    .filter(F.hour("log_ts").between(8, 14))
    .select(
        "id",
        "level",
        "k",
        "k_length",
        "k_filled",
        F.stddev_samp("k_length").over(w_recent).alias("level_stddev_samp_k_length"),
    )
)

with_windows = (
    metrics
    .select(
        "id",
        "k_length",
        "k_filled",
        F.percent_rank().over(w2).alias("k_length_percent_rank"),
        F.cume_dist().over(w3).alias("k_length_cume_dist"),
        F.rank().over(w4).alias("k_length_rank"),
        F.max("k_length").over(w_recent).alias("level_max_k_length"),
        F.dense_rank().over(w5).alias("level_stddev_samp_k_length_dense_rank"),
    )
)

result = (
    with_windows
    .groupBy(
        F.when(F.col("k_length_rank") >= 7, "LARGE").otherwise("SMALL").alias("k_length_rank_tier"),
    )
    .agg(
        F.percentile_approx("level_max_k_length", [0.25, 0.5, 0.75]).alias("level_max_k_length_quartiles"),
        F.percentile_approx("k_length_cume_dist", 0.95).alias("k_length_cume_dist_p95"),
        F.avg("k_length_rank").alias("avg_k_length_rank"),
    )
    .filter(F.col("avg_k_length_rank") > 0)
    .orderBy(F.col("k_length_cume_dist_p95").asc_nulls_first())
)
