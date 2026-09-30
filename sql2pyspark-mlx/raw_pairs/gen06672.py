from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("latency_ms").asc_nulls_last())
w2 = Window.partitionBy("k", "host").orderBy(F.col("latency_ms_percent_rank").asc_nulls_first())
w3 = Window.partitionBy("k")

flattened = (
    df_app_logs
    .select(
        "id",
        "host",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("k", "attr_value"),
    )
)

metrics = flattened.select("*", F.percent_rank().over(w).alias("latency_ms_percent_rank"))

enriched = (
    metrics
    .filter(F.col("latency_ms").between(1395, 1467))
    .select(
        "latency_ms",
        "attr_value",
        F.percent_rank().over(w2).alias("latency_ms_percent_rank_percent_rank"),
        F.round((F.col("latency_ms") / F.sum("latency_ms").over(w3)), 4).alias("latency_ms_share"),
    )
)

base = (
    enriched
    .filter(F.col("latency_ms_percent_rank_percent_rank") < 0.76)
    .select("*", F.ceil("latency_ms_share").alias("latency_ms_share_whole"))
)

filtered = (
    base
    .filter(F.col("latency_ms").between(771, 1649))
    .select(
        "latency_ms",
        "attr_value",
        "latency_ms_percent_rank_percent_rank",
        "latency_ms_share_whole",
        (F.col("latency_ms") * F.col("latency_ms_share_whole")).alias("latency_ms_times_latency_ms_share_whole"),
    )
)

result = (
    filtered
    .groupBy(
        F.when(F.col("latency_ms") >= 2665, "PREMIUM").otherwise("STANDARD").alias("latency_ms_tier"),
        F.when(F.col("latency_ms_percent_rank_percent_rank") >= 0.6, "PREMIUM").otherwise("STANDARD").alias("latency_ms_percent_rank_percent_rank_tier"),
    )
    .agg(
        F.percentile_approx("latency_ms", 0.9).alias("latency_ms_p90"),
        F.percentile_approx("latency_ms_percent_rank_percent_rank", 0.95).alias("latency_ms_percent_rank_percent_rank_p95"),
        F.percentile_approx("latency_ms_share_whole", 0.9).alias("latency_ms_share_whole_p90"),
        F.round(F.avg("latency_ms_times_latency_ms_share_whole"), 2).alias("avg_latency_ms_times_latency_ms_share_whole"),
    )
)
