from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("status_code").desc())
w_recent = Window.orderBy(F.col("flag_bit8").asc_nulls_last())
w3 = Window.orderBy(F.col("flag_bit8").asc_nulls_first())
w_hist = Window.orderBy("status_code")

lines = (
    df_app_logs
    .select(
        "id",
        "host",
        "level",
        "status_code",
        F.explode_outer("context").alias("attr_key", "v"),
    )
)

parsed = (
    lines
    .filter(F.col("status_code").between(263, 416))
    .select(
        "status_code",
        F.shiftright("status_code", 2).alias("status_code_high"),
        (F.col("status_code").bitwiseAND(8) != 0).cast("int").alias("flag_bit8"),
    )
)

with_windows = (
    parsed
    .filter(F.col("status_code_high") != 1)
    .select(
        "status_code",
        "flag_bit8",
        F.rank().over(w).alias("status_code_rank"),
        F.dense_rank().over(w_recent).alias("flag_bit8_dense_rank"),
        F.percent_rank().over(w_recent).alias("flag_bit8_percent_rank"),
        F.dense_rank().over(w3).alias("flag_bit8_dense_rank_2"),
        F.rank().over(w_recent).alias("flag_bit8_rank"),
    )
)

with_windows_app_logs = (
    with_windows
    .select(
        "flag_bit8",
        "flag_bit8_percent_rank",
        "flag_bit8_rank",
        F.rank().over(w_hist).alias("status_code_rank_2"),
    )
)

result = (
    with_windows_app_logs
    .groupBy(
        F.when(F.col("flag_bit8_percent_rank") >= 0.73, "HIGH").otherwise("LOW").alias("flag_bit8_percent_rank_tier"),
    )
    .agg(
        F.round(F.avg("status_code_rank_2"), 2).alias("avg_status_code_rank_2"),
        F.percentile_approx("flag_bit8", 0.95).alias("flag_bit8_p95"),
        F.percentile_approx("status_code_rank_2", 0.95).alias("status_code_rank_2_p95"),
        F.percentile_approx("flag_bit8_percent_rank", 0.5).alias("flag_bit8_percent_rank_p50"),
        F.count("*").alias("row_count"),
    )
    .orderBy(F.col("avg_status_code_rank_2").desc())
)
