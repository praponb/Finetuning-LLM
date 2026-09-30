from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("k").orderBy(F.col("latency_ms").asc_nulls_first())
w_hist = Window.partitionBy("k").orderBy("latency_ms_cume_dist")
w3 = Window.partitionBy("k").orderBy(F.col("latency_ms_cume_dist").desc_nulls_first())

lines = (
    df_app_logs
    .select("id", "log_ts", "latency_ms", F.explode_outer("context").alias("k", "v"))
)

with_windows = (
    lines
    .select("latency_ms", "k", "v", F.cume_dist().over(w).alias("latency_ms_cume_dist"))
)

metrics = (
    with_windows
    .select(
        "latency_ms",
        "k",
        "latency_ms_cume_dist",
        F.percent_rank().over(w_hist).alias("latency_ms_cume_dist_percent_rank"),
        F.dense_rank().over(w3).alias("latency_ms_cume_dist_dense_rank"),
    )
)

valid = (
    metrics
    .filter(F.col("k").isNotNull() & (F.col("latency_ms_cume_dist_dense_rank") > 7))
    .select(
        "k",
        "latency_ms_cume_dist",
        "latency_ms_cume_dist_percent_rank",
        "latency_ms_cume_dist_dense_rank",
        F.col("latency_ms_cume_dist_dense_rank").cast("double").alias("latency_ms_cume_dist_dense_rank_num"),
        F.coalesce("k", F.lit("UNKNOWN")).alias("k_filled"),
    )
)

filtered = (
    valid
    .filter(~F.col("k").eqNullSafe(F.lit("version")))
    .withColumn("latency_ms_cume_dist_dense_rank_filled", F.coalesce("latency_ms_cume_dist_dense_rank", F.lit(0)))
    .withColumn("k_filled_2", F.coalesce("k", F.lit("UNKNOWN")))
)

stats = (
    filtered
    .groupBy(
        F.when(F.col("latency_ms_cume_dist") >= 0.73, "HIGH").otherwise("LOW").alias("latency_ms_cume_dist_tier"),
    )
    .agg(
        F.sum(F.when(F.col("k") == "trace", F.col("latency_ms_cume_dist_dense_rank_filled")).otherwise(0)).alias("trace_latency_ms_cume_dist_dense_rank_filled"),
        F.percentile_approx("latency_ms_cume_dist_dense_rank_num", 0.5).alias("latency_ms_cume_dist_dense_rank_num_p50"),
        F.percentile_approx("latency_ms_cume_dist_percent_rank", 0.9).alias("latency_ms_cume_dist_percent_rank_p90"),
    )
)

result = (
    stats
    .select("latency_ms_cume_dist_tier", "latency_ms_cume_dist_dense_rank_num_p50")
    .orderBy(F.col("latency_ms_cume_dist_tier").desc_nulls_last())
    .limit(20)
)
