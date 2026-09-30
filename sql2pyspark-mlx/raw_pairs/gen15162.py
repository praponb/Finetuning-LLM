from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("k")
w2 = Window.partitionBy("k").orderBy(F.col("latency_ms").desc_nulls_first())
w3 = Window.partitionBy("k").orderBy(F.col("k_sum_latency_ms").asc_nulls_first())

lines = (
    df_app_logs
    .select("id", "host", "latency_ms", F.explode_outer("context").alias("k", "v"))
)

cleaned = (
    lines
    .filter(F.col("k").isNotNull() & (~F.col("k").eqNullSafe(F.lit("version"))))
    .select(
        "id",
        "latency_ms",
        "k",
        "v",
        F.when(F.col("host").isin(["h2", "h4"]), "Core").otherwise("Other").alias("host_group"),
        F.coalesce("k", F.lit("DIRECT")).alias("resolved_k"),
    )
)

base = (
    cleaned
    .filter(F.col("k").isNotNull())
    .select(
        "id",
        "latency_ms",
        "k",
        "v",
        "host_group",
        "resolved_k",
        F.when(F.col("latency_ms") >= 2115, "High").when(F.col("latency_ms") >= 872, "Medium").otherwise("Low").alias("latency_ms_level"),
        F.coalesce("k", F.lit("N/A")).alias("k_clean"),
    )
)

metrics = (
    base
    .select(
        "id",
        "latency_ms",
        "k",
        "host_group",
        F.sum("latency_ms").over(w_running).alias("k_sum_latency_ms"),
        F.cume_dist().over(w2).alias("latency_ms_cume_dist"),
    )
)

base_app_logs = (
    metrics
    .filter(F.col("k").isNotNull())
    .select(
        "id",
        "latency_ms",
        "k",
        "k_sum_latency_ms",
        "latency_ms_cume_dist",
        F.when(F.col("k").isin(["version", "region"]), "Group A").otherwise("Other").alias("k_group"),
        F.coalesce("k", F.lit("UNKNOWN")).alias("k_clean"),
    )
)

metrics_app_logs = (
    base_app_logs
    .select(
        "k",
        "k_group",
        "k_clean",
        F.cume_dist().over(w3).alias("k_sum_latency_ms_cume_dist"),
    )
)

enriched = (
    metrics_app_logs
    .select(
        "k_group",
        "k_clean",
        F.stddev_samp("k_sum_latency_ms_cume_dist").over(w_running).alias("k_stddev_samp_k_sum_latency_ms_cume_dist"),
    )
)

result = (
    enriched
    .groupBy(
        F.when(F.col("k_stddev_samp_k_sum_latency_ms_cume_dist") >= 1, "PREMIUM").otherwise("STANDARD").alias("k_stddev_samp_k_sum_latency_ms_cume_dist_tier"),
    )
    .agg(
        F.percentile_approx("k_stddev_samp_k_sum_latency_ms_cume_dist", [0.25, 0.5, 0.75]).alias("k_stddev_samp_k_sum_latency_ms_cume_dist_quartiles"),
        F.percentile_approx("k_stddev_samp_k_sum_latency_ms_cume_dist", 0.9).alias("k_stddev_samp_k_sum_latency_ms_cume_dist_p90"),
        F.percentile_approx("k_stddev_samp_k_sum_latency_ms_cume_dist", [0.25, 0.5, 0.75]).alias("k_stddev_samp_k_sum_latency_ms_cume_dist_quartiles_2"),
        F.count("*").alias("num_rows"),
        F.round(F.avg("k_stddev_samp_k_sum_latency_ms_cume_dist"), 2).alias("avg_k_stddev_samp_k_sum_latency_ms_cume_dist"),
        F.round(F.avg("k_stddev_samp_k_sum_latency_ms_cume_dist"), 2).alias("avg_k_stddev_samp_k_sum_latency_ms_cume_dist_2"),
    )
)
