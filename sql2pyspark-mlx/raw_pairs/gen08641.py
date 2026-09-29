from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("k").orderBy(F.col("latency_ms").asc_nulls_first())
w2 = Window.partitionBy("k")
w3 = Window.orderBy("latency_ms")
w4 = Window.orderBy(F.col("latency_ms_cume_dist_minus_latency_ms_percent_rank").asc_nulls_first())
w5 = Window.partitionBy("service")
w6 = Window.partitionBy("host", "service").orderBy(F.col("latency_ms_percent_rank").desc_nulls_first())
w7 = Window.orderBy(F.col("status_code").desc())
w_grp = Window.partitionBy("service", "k")

flattened = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("k", "attr_value"),
    )
)

enriched = (
    flattened
    .filter(F.col("status_code") > 375)
    .select(
        "*",
        F.cume_dist().over(w_running).alias("latency_ms_cume_dist"),
        F.corr("latency_ms", "status_code").over(w2).alias("corr_latency_ms_status_code"),
        F.percent_rank().over(w3).alias("latency_ms_percent_rank"),
    )
)

base = (
    enriched
    .filter(F.col("latency_ms_percent_rank") <= 0.71)
    .select(
        "*",
        F.ceil("corr_latency_ms_status_code").alias("corr_latency_ms_status_code_whole"),
        (F.col("latency_ms_cume_dist") - F.col("latency_ms_percent_rank")).alias("latency_ms_cume_dist_minus_latency_ms_percent_rank"),
    )
)

with_windows = (
    base
    .select(
        "*",
        F.rank().over(w4).alias("latency_ms_cume_dist_minus_latency_ms_percent_rank_rank"),
        F.count("status_code").over(w5).alias("service_count_status_code"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "host",
        "latency_ms",
        "k",
        "latency_ms_cume_dist",
        "corr_latency_ms_status_code",
        "corr_latency_ms_status_code_whole",
        "service_count_status_code",
        F.percent_rank().over(w6).alias("latency_ms_percent_rank_percent_rank"),
        F.rank().over(w7).alias("status_code_rank"),
        F.corr("latency_ms", "status_code").over(w_grp).alias("corr_latency_ms_status_code_2"),
    )
)
