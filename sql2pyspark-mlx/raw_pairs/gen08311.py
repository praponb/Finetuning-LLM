from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("flag_bit8").asc_nulls_last())
w2 = Window.orderBy(F.col("status_code_high").asc_nulls_last())
w3 = Window.orderBy(F.col("flag_bit8").desc_nulls_last())
w4 = Window.orderBy(F.col("status_code_high_percent_rank").desc())
w5 = Window.orderBy("status_code")
w_running = Window.orderBy(F.col("flag_bit8").desc_nulls_first())
w7 = Window.orderBy("status_code_high_dense_rank")

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "log_ts",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("key", "v"),
    )
)

flattened_attrs = (
    exploded
    .select(
        "status_code",
        "v",
        F.shiftright("status_code", 2).alias("status_code_high"),
        (F.col("status_code").bitwiseAND(8) != 0).cast("int").alias("flag_bit8"),
    )
)

metrics = (
    flattened_attrs
    .select(
        "*",
        F.dense_rank().over(w).alias("flag_bit8_dense_rank"),
        F.percent_rank().over(w2).alias("status_code_high_percent_rank"),
        F.rank().over(w3).alias("flag_bit8_rank"),
        F.rank().over(w2).alias("status_code_high_rank"),
    )
)

with_windows = (
    metrics
    .select(
        "*",
        F.percent_rank().over(w4).alias("status_code_high_percent_rank_percent_rank"),
        F.percent_rank().over(w5).alias("status_code_percent_rank"),
        F.dense_rank().over(w_running).alias("flag_bit8_dense_rank_2"),
        F.dense_rank().over(w2).alias("status_code_high_dense_rank"),
    )
)

valid = (
    with_windows
    .filter(F.col("status_code_high_percent_rank") >= 0.41)
    .select(
        "v",
        "status_code_high",
        "flag_bit8_dense_rank",
        "status_code_high_percent_rank",
        "flag_bit8_rank",
        "status_code_high_rank",
        "status_code_high_percent_rank_percent_rank",
        "status_code_percent_rank",
        "flag_bit8_dense_rank_2",
        "status_code_high_dense_rank",
        F.coalesce("status_code_high_percent_rank_percent_rank", F.lit(0)).alias("status_code_high_percent_rank_percent_rank_filled"),
    )
)

with_windows_app_logs = (
    valid
    .select(
        "v",
        "status_code_high",
        "flag_bit8_dense_rank",
        "status_code_high_percent_rank",
        "status_code_high_percent_rank_percent_rank",
        F.rank().over(w7).alias("status_code_high_dense_rank_rank"),
    )
)

result = (
    with_windows_app_logs
    .groupBy(
        F.when(F.col("status_code_high") >= 3, "LARGE").otherwise("SMALL").alias("status_code_high_tier"),
    )
    .agg(
        F.percentile_approx("status_code_high_percent_rank_percent_rank", [0.25, 0.5, 0.75]).alias("status_code_high_percent_rank_percent_rank_quartiles"),
        F.min("status_code_high_percent_rank").alias("min_status_code_high_percent_rank"),
        F.round(F.avg("flag_bit8_dense_rank"), 2).alias("avg_flag_bit8_dense_rank"),
        F.percentile_approx("status_code_high_percent_rank_percent_rank", 0.95).alias("status_code_high_percent_rank_percent_rank_p95"),
        F.count("*").alias("event_count"),
        F.count("*").alias("cnt"),
    )
    .orderBy("cnt")
)
