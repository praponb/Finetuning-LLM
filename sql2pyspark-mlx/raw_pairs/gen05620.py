from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("host", "level")
w_grp = Window.partitionBy("attr_key")
w3 = Window.partitionBy("attr_key", "host").orderBy("status_code")
w_recent = Window.partitionBy("host").orderBy(F.col("corr_latency_ms_status_code").desc())
w5 = Window.orderBy(F.col("corr_latency_ms_status_code").desc_nulls_last())
w6 = Window.orderBy(F.col("corr_latency_ms_status_code").desc())
w7 = Window.partitionBy("host").orderBy(F.col("corr_latency_ms_status_code_cume_dist").desc())
w_ordered = Window.partitionBy("host", "attr_key")
w9 = Window.partitionBy("host")
w10 = Window.partitionBy("attr_key").orderBy(F.col("status_code").desc())
w_part = Window.orderBy(F.col("status_code_cume_dist").desc_nulls_first())

exploded = (
    df_app_logs
    .select(
        "id",
        "host",
        "level",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("attr_key", "v"),
    )
)

with_windows = (
    exploded
    .select(
        "host",
        "status_code",
        "attr_key",
        F.corr("latency_ms", "status_code").over(w).alias("corr_latency_ms_status_code"),
    )
)

metrics = (
    with_windows
    .filter(F.col("attr_key") == "region")
    .withColumn("attr_key_sum_corr_latency_ms_status_code", F.sum("corr_latency_ms_status_code").over(w_grp))
    .withColumn("status_code_rank", F.rank().over(w3))
    .withColumn("corr_latency_ms_status_code_cume_dist", F.cume_dist().over(w_recent))
)

metrics_app_logs = (
    metrics
    .withColumn("status_code_rank_share", F.round((F.col("status_code_rank") / F.sum("status_code_rank").over(w_grp)), 4))
    .withColumn("corr_latency_ms_status_code_cume_dist_2", F.cume_dist().over(w5))
    .withColumn("corr_latency_ms_status_code_rank", F.rank().over(w6))
    .withColumn("corr_latency_ms_status_code_cume_dist_percent_rank", F.percent_rank().over(w7))
)

metrics_app_logs_2 = (
    metrics_app_logs
    .select(
        "host",
        "corr_latency_ms_status_code",
        "attr_key_sum_corr_latency_ms_status_code",
        "status_code_rank",
        "corr_latency_ms_status_code_cume_dist",
        "corr_latency_ms_status_code_rank",
        F.avg("attr_key_sum_corr_latency_ms_status_code").over(w_ordered).alias("host_avg_attr_key_sum_corr_latency_ms_status_code"),
        F.stddev_samp("corr_latency_ms_status_code_cume_dist_2").over(w9).alias("host_stddev_samp_corr_latency_ms_status_code_cume_dist_2"),
        F.avg("status_code_rank_share").over(w9).alias("host_avg_status_code_rank_share"),
        F.avg("corr_latency_ms_status_code_cume_dist_percent_rank").over(w9).alias("host_avg_corr_latency_ms_status_code_cume_dist_percent_rank"),
        F.cume_dist().over(w10).alias("status_code_cume_dist"),
    )
)

result = (
    metrics_app_logs_2
    .select(
        "corr_latency_ms_status_code",
        "attr_key_sum_corr_latency_ms_status_code",
        "status_code_rank",
        "corr_latency_ms_status_code_cume_dist",
        "corr_latency_ms_status_code_rank",
        "host_avg_attr_key_sum_corr_latency_ms_status_code",
        "host_stddev_samp_corr_latency_ms_status_code_cume_dist_2",
        "status_code_cume_dist",
        F.sum("host_stddev_samp_corr_latency_ms_status_code_cume_dist_2").over(w9).alias("host_sum_host_stddev_samp_corr_latency_ms_status_code_cume_dist_2"),
        F.percent_rank().over(w_part).alias("status_code_cume_dist_percent_rank"),
    )
)
