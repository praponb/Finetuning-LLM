from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("status_code_high").asc_nulls_last())
w2 = Window.orderBy(F.col("status_code_high").desc())
w3 = Window.orderBy(F.col("latency_ms").asc_nulls_first())
w4 = Window.orderBy(F.col("status_code_high_rank").desc_nulls_last())
w_hist = Window.orderBy(F.col("status_code_high_rank_per_latency_ms_percent_rank").desc())
w6 = Window.orderBy(F.col("latency_ms_percent_rank").desc_nulls_first())
w_grp = Window.orderBy("status_code_high_cume_dist")

flattened = (
    df_app_logs
    .select(
        "id",
        "service",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("attr_key", "value"),
    )
)

unpacked = (
    flattened
    .select(
        "id",
        "latency_ms",
        (F.col("status_code").bitwiseAND(1) != 0).cast("int").alias("is_flagged"),
        F.shiftright("status_code", 2).alias("status_code_high"),
    )
)

scored = (
    unpacked
    .filter(F.col("latency_ms") == 1649)
    .select(
        "latency_ms",
        "is_flagged",
        "status_code_high",
        F.rank().over(w).alias("status_code_high_rank"),
        F.cume_dist().over(w2).alias("status_code_high_cume_dist"),
        F.percent_rank().over(w3).alias("latency_ms_percent_rank"),
    )
)

valid = (
    scored
    .filter(F.col("status_code_high_rank").between(4, 6))
    .select(
        "latency_ms",
        "is_flagged",
        "status_code_high",
        "status_code_high_rank",
        "status_code_high_cume_dist",
        "latency_ms_percent_rank",
        (F.col("latency_ms_percent_rank") - F.col("latency_ms")).alias("latency_ms_percent_rank_minus_latency_ms"),
        (F.col("status_code_high_rank") / F.nullif("latency_ms_percent_rank", F.lit(0))).alias("status_code_high_rank_per_latency_ms_percent_rank"),
    )
)

metrics = (
    valid
    .withColumn("status_code_high_rank_cume_dist", F.cume_dist().over(w4))
    .withColumn("status_code_high_rank_per_latency_ms_percent_rank_percent_rank", F.percent_rank().over(w_hist))
    .withColumn("latency_ms_percent_rank_dense_rank", F.dense_rank().over(w6))
    .withColumn("status_code_high_cume_dist_dense_rank", F.dense_rank().over(w_grp))
)

valid_app_logs = (
    metrics
    .filter(F.col("status_code_high_rank_cume_dist") >= 0.32)
    .select(
        "latency_ms",
        "is_flagged",
        "status_code_high",
        "status_code_high_cume_dist",
        "latency_ms_percent_rank",
        "latency_ms_percent_rank_minus_latency_ms",
        "status_code_high_rank_per_latency_ms_percent_rank",
        "status_code_high_rank_cume_dist",
        "latency_ms_percent_rank_dense_rank",
        "status_code_high_cume_dist_dense_rank",
        F.round("status_code_high_rank_per_latency_ms_percent_rank_percent_rank", 0).alias("status_code_high_rank_per_latency_ms_percent_rank_percent_rank_rounded"),
    )
)

result = (
    valid_app_logs
    .select(
        "latency_ms",
        "status_code_high",
        "latency_ms_percent_rank",
        "latency_ms_percent_rank_minus_latency_ms",
        "status_code_high_rank_cume_dist",
        "latency_ms_percent_rank_dense_rank",
        "status_code_high_cume_dist_dense_rank",
    )
)
