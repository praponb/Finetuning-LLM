from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("host")
w2 = Window.orderBy(F.col("status_code").desc())
w3 = Window.partitionBy("attr_key", "service").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-7200, Window.currentRow)
w_running = Window.partitionBy("service").orderBy(F.col("status_code").desc())
w_running_2 = Window.partitionBy("service").orderBy(F.col("status_code_cume_dist_sum_2h").desc_nulls_first())
w6 = Window.partitionBy("service")
w7 = Window.orderBy(F.col("service_stddev_samp_corr_status_code_latency_ms").asc_nulls_first())
w8 = Window.orderBy(F.col("service_stddev_samp_corr_status_code_latency_ms_dense_rank").desc_nulls_last())

flattened = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("attr_key", "value"),
    )
)

metrics = (
    flattened
    .select(
        "*",
        F.corr("status_code", "latency_ms").over(w).alias("corr_status_code_latency_ms"),
        F.cume_dist().over(w2).alias("status_code_cume_dist"),
    )
)

enriched = (
    metrics
    .select(
        "id",
        "service",
        "status_code",
        "value",
        "corr_status_code_latency_ms",
        "status_code_cume_dist",
        F.sum("status_code_cume_dist").over(w3).alias("status_code_cume_dist_sum_2h"),
    )
)

enriched_app_logs = (
    enriched
    .select(
        "*",
        F.percent_rank().over(w_running).alias("status_code_percent_rank"),
        F.dense_rank().over(w_running_2).alias("status_code_cume_dist_sum_2h_dense_rank"),
        F.stddev_samp("corr_status_code_latency_ms").over(w6).alias("service_stddev_samp_corr_status_code_latency_ms"),
    )
)

metrics_app_logs = (
    enriched_app_logs
    .select(
        "*",
        F.avg("status_code_percent_rank").over(w6).alias("service_avg_status_code_percent_rank"),
        F.dense_rank().over(w7).alias("service_stddev_samp_corr_status_code_latency_ms_dense_rank"),
    )
)

scored = (
    metrics_app_logs
    .filter(F.col("status_code_percent_rank") < 0.45)
    .select(
        "id",
        "status_code",
        "value",
        "corr_status_code_latency_ms",
        "status_code_cume_dist",
        "status_code_cume_dist_sum_2h",
        "status_code_percent_rank",
        "status_code_cume_dist_sum_2h_dense_rank",
        "service_stddev_samp_corr_status_code_latency_ms",
        "service_stddev_samp_corr_status_code_latency_ms_dense_rank",
        F.cume_dist().over(w8).alias("service_stddev_samp_corr_status_code_latency_ms_dense_rank_cume_dist"),
    )
)

cleaned = (
    scored
    .filter(F.col("service_stddev_samp_corr_status_code_latency_ms_dense_rank_cume_dist") > 0.28)
    .withColumn("status_code_cume_dist_whole", F.floor("status_code_cume_dist"))
)

result = (
    cleaned
    .withColumn("corr_status_code_latency_ms_tier", F.when(F.col("corr_status_code_latency_ms") >= 0.67, "HIGH").otherwise("LOW"))
    .withColumn("service_stddev_samp_corr_status_code_latency_ms_dense_rank_tier", F.when(F.col("service_stddev_samp_corr_status_code_latency_ms_dense_rank") >= 7, "PREMIUM").otherwise("STANDARD"))
    .groupingSets(
        [["corr_status_code_latency_ms_tier", "service_stddev_samp_corr_status_code_latency_ms_dense_rank_tier"], ["corr_status_code_latency_ms_tier"], []],
        "corr_status_code_latency_ms_tier",
        "service_stddev_samp_corr_status_code_latency_ms_dense_rank_tier",
    )
    .agg(
        F.percentile_approx("service_stddev_samp_corr_status_code_latency_ms_dense_rank_cume_dist", [0.25, 0.5, 0.75]).alias("service_stddev_samp_corr_status_code_latency_ms_dense_rank_cume_dist_quartiles"),
        F.min("service_stddev_samp_corr_status_code_latency_ms_dense_rank").alias("min_service_stddev_samp_corr_status_code_latency_ms_dense_rank"),
        F.round(F.avg("status_code_percent_rank"), 2).alias("avg_status_code_percent_rank"),
        F.grouping_id().alias("grouping_id"),
    )
    .filter(F.col("avg_status_code_percent_rank") >= 0)
    .orderBy(F.col("corr_status_code_latency_ms_tier").desc_nulls_last())
)
