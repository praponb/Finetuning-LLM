from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("k")
w2 = Window.partitionBy("host")
w3 = Window.partitionBy("host", "service")
w4 = Window.partitionBy("host").orderBy(F.col("latency_ms").desc_nulls_last())
w_ordered = Window.partitionBy("service", "host").orderBy(F.col("k_avg_latency_ms_share").asc_nulls_last())
w_hist = Window.partitionBy("service")
w7 = Window.orderBy(F.col("latency_ms").asc_nulls_first())
w8 = Window.partitionBy("service", "host")
w9 = Window.orderBy(F.col("k_avg_latency_ms_share").desc())

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("k", "value"),
    )
)

enriched = (
    exploded
    .select(
        "id",
        "service",
        "host",
        "latency_ms",
        "status_code",
        F.avg("latency_ms").over(w).alias("k_avg_latency_ms"),
    )
)

metrics = (
    enriched
    .select(
        "*",
        F.round((F.col("status_code") / F.sum("status_code").over(w2)), 4).alias("status_code_share"),
        F.round((F.col("k_avg_latency_ms") / F.sum("k_avg_latency_ms").over(w2)), 4).alias("k_avg_latency_ms_share"),
        F.min("k_avg_latency_ms").over(w2).alias("host_min_k_avg_latency_ms"),
        F.max("k_avg_latency_ms").over(w3).alias("host_max_k_avg_latency_ms"),
        F.rank().over(w4).alias("latency_ms_rank"),
    )
)

with_windows = (
    metrics
    .filter(F.col("host_min_k_avg_latency_ms") <= 1153)
    .select(
        "*",
        F.dense_rank().over(w_ordered).alias("k_avg_latency_ms_share_dense_rank"),
        F.min("host_min_k_avg_latency_ms").over(w_hist).alias("service_min_host_min_k_avg_latency_ms"),
        F.avg("k_avg_latency_ms_share").over(w3).alias("host_avg_k_avg_latency_ms_share"),
        F.percent_rank().over(w7).alias("latency_ms_percent_rank"),
        F.sum("host_max_k_avg_latency_ms").over(w_hist).alias("service_sum_host_max_k_avg_latency_ms"),
    )
)

metrics_app_logs = (
    with_windows
    .select(
        "*",
        F.corr("status_code", "latency_ms").over(w_hist).alias("corr_status_code_latency_ms"),
        F.round((F.col("host_max_k_avg_latency_ms") / F.sum("host_max_k_avg_latency_ms").over(w8)), 4).alias("host_max_k_avg_latency_ms_share"),
        F.cume_dist().over(w9).alias("k_avg_latency_ms_share_cume_dist"),
    )
)

result = (
    metrics_app_logs
    .groupBy(
        F.when(F.col("host_max_k_avg_latency_ms_share") >= 0.72, "HIGH").otherwise("LOW").alias("host_max_k_avg_latency_ms_share_tier"),
    )
    .agg(
        F.percentile_approx("host_min_k_avg_latency_ms", [0.25, 0.5, 0.75]).alias("host_min_k_avg_latency_ms_quartiles"),
        F.percentile_approx("host_min_k_avg_latency_ms", 0.9).alias("host_min_k_avg_latency_ms_p90"),
        F.sort_array(F.collect_set("service")).alias("service_list"),
        F.round(F.sum("host_min_k_avg_latency_ms") / F.nullif(F.count(F.when(F.col("host") == "h4", 1)), F.lit(0)), 2).alias("host_min_k_avg_latency_ms_per_h4"),
    )
    .orderBy(F.col("host_max_k_avg_latency_ms_share_tier").desc_nulls_first())
)
