from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("latency_ms").desc_nulls_last())
w2 = Window.orderBy("status_code")
w3 = Window.partitionBy("host")
w4 = Window.orderBy("latency_ms")
w5 = Window.orderBy(F.col("host_max_status_code_plus_latency_ms").desc_nulls_first())
w6 = Window.orderBy(F.col("host_max_status_code_plus_latency_ms_filled").desc_nulls_first())
w7 = Window.orderBy(F.col("host_max_status_code_plus_latency_ms_rank").desc())
w8 = Window.orderBy(F.col("host_max_status_code_plus_latency_ms_filled").desc_nulls_last())
w_part = Window.orderBy(F.col("latency_ms_cume_dist").desc())
w_running = Window.orderBy("host_max_status_code_plus_latency_ms_rank")

lines = (
    df_app_logs
    .select(
        "id",
        "host",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("k", "v"),
    )
)

base = (
    lines
    .filter(~F.col("k").eqNullSafe(F.lit("version")))
    .select(
        "id",
        "host",
        "latency_ms",
        "status_code",
        "k",
        F.substring("host", 1, 3).alias("host_prefix"),
        (F.col("status_code") + F.col("latency_ms")).alias("status_code_plus_latency_ms"),
    )
)

metrics = (
    base
    .select(
        "id",
        "latency_ms",
        "status_code",
        "host_prefix",
        "status_code_plus_latency_ms",
        F.cume_dist().over(w).alias("latency_ms_cume_dist"),
        F.rank().over(w2).alias("status_code_rank"),
        F.max("status_code_plus_latency_ms").over(w3).alias("host_max_status_code_plus_latency_ms"),
        F.cume_dist().over(w4).alias("latency_ms_cume_dist_2"),
    )
)

valid = (
    metrics
    .filter(F.col("latency_ms_cume_dist") < 0.31)
    .select(
        "id",
        "status_code",
        "host_prefix",
        "status_code_plus_latency_ms",
        "latency_ms_cume_dist",
        "status_code_rank",
        "host_max_status_code_plus_latency_ms",
        "latency_ms_cume_dist_2",
        F.coalesce("host_max_status_code_plus_latency_ms", F.lit(0)).alias("host_max_status_code_plus_latency_ms_filled"),
    )
)

metrics_app_logs = (
    valid
    .filter(F.col("latency_ms_cume_dist") < 0.24)
    .select(
        "latency_ms_cume_dist",
        "host_max_status_code_plus_latency_ms_filled",
        F.rank().over(w5).alias("host_max_status_code_plus_latency_ms_rank"),
    )
)

scored = (
    metrics_app_logs
    .select(
        "latency_ms_cume_dist",
        "host_max_status_code_plus_latency_ms_filled",
        F.rank().over(w6).alias("host_max_status_code_plus_latency_ms_filled_rank"),
        F.dense_rank().over(w7).alias("host_max_status_code_plus_latency_ms_rank_dense_rank"),
        F.dense_rank().over(w8).alias("host_max_status_code_plus_latency_ms_filled_dense_rank"),
        F.cume_dist().over(w_part).alias("latency_ms_cume_dist_cume_dist"),
        F.dense_rank().over(w_running).alias("host_max_status_code_plus_latency_ms_rank_dense_rank_2"),
    )
)

result = (
    scored
    .select(
        "latency_ms_cume_dist",
        "host_max_status_code_plus_latency_ms_filled",
        "host_max_status_code_plus_latency_ms_filled_dense_rank",
        "host_max_status_code_plus_latency_ms_rank_dense_rank_2",
    )
)
