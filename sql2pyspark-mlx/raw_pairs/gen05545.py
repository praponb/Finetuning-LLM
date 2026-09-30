from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("status_code_high").desc_nulls_first())
w2 = Window.orderBy(F.col("status_code_high_rank").desc())
w3 = Window.orderBy(F.col("status_code_high").asc_nulls_first())
w4 = Window.orderBy(F.col("status_code_high_rank").desc_nulls_last())
w5 = Window.orderBy(F.col("flag_bit2").asc_nulls_last())
w6 = Window.orderBy(F.col("status_code_high_rank_cume_dist").desc_nulls_first())

flattened = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "level",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("attr_key", "value"),
    )
)

flattened_attrs = (
    flattened
    .select(
        "value",
        (F.col("status_code").bitwiseAND(2) != 0).cast("int").alias("flag_bit2"),
        F.shiftright("status_code", 2).alias("status_code_high"),
    )
)

metrics = flattened_attrs.select("*", F.rank().over(w).alias("status_code_high_rank"))

metrics_app_logs = (
    metrics
    .select(
        "*",
        F.cume_dist().over(w2).alias("status_code_high_rank_cume_dist"),
        F.cume_dist().over(w3).alias("status_code_high_cume_dist"),
        F.dense_rank().over(w4).alias("status_code_high_rank_dense_rank"),
    )
)

with_windows = (
    metrics_app_logs
    .select(
        "value",
        "status_code_high",
        "status_code_high_rank",
        "status_code_high_rank_cume_dist",
        "status_code_high_cume_dist",
        "status_code_high_rank_dense_rank",
        F.cume_dist().over(w5).alias("flag_bit2_cume_dist"),
        F.percent_rank().over(w6).alias("status_code_high_rank_cume_dist_percent_rank"),
    )
)

result = (
    with_windows
    .groupBy(
        F.when(F.col("status_code_high_cume_dist") >= 0.85, "LARGE").otherwise("SMALL").alias("status_code_high_cume_dist_tier"),
    )
    .agg(
        F.percentile_approx("status_code_high_rank_cume_dist", 0.5).alias("status_code_high_rank_cume_dist_p50"),
        F.avg("status_code_high_rank").alias("avg_status_code_high_rank"),
        F.percentile_approx("status_code_high_rank_dense_rank", [0.25, 0.5, 0.75]).alias("status_code_high_rank_dense_rank_quartiles"),
        F.round(F.avg("status_code_high_rank_cume_dist_percent_rank"), 2).alias("avg_status_code_high_rank_cume_dist_percent_rank"),
        F.round(F.avg("status_code_high_rank_cume_dist"), 2).alias("avg_status_code_high_rank_cume_dist"),
    )
    .filter(F.col("avg_status_code_high_rank_cume_dist") > 0)
    .orderBy(F.col("avg_status_code_high_rank_cume_dist_percent_rank").desc())
)
