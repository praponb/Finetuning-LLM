from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("currency").orderBy("currency_length")
w2 = Window.orderBy(F.col("currency_length").asc_nulls_first())
w3 = Window.orderBy(F.col("currency_length_dense_rank").desc_nulls_first())
w_grp = Window.orderBy(F.col("currency_length").desc())
w5 = Window.orderBy(F.col("currency_length_percent_rank").desc())
w_part = Window.partitionBy(F.year("effective_from"))
w7 = Window.orderBy("val")

lines = (
    df_price_tiers
    .select("id", "effective_from", "currency", F.explode("tier_limits").alias("val"))
)

cleaned = (
    lines
    .filter(F.col("effective_from") < "2024-10-28")
    .select("*", F.length("currency").alias("currency_length"))
)

filtered = (
    cleaned
    .filter(F.year("effective_from") >= 2024)
    .select("*", F.col("val").cast("string").alias("val_str"))
)

with_windows = (
    filtered
    .filter(F.col("val") < 65)
    .select(
        "effective_from",
        "val",
        "currency_length",
        F.percent_rank().over(w).alias("currency_length_percent_rank"),
        F.dense_rank().over(w2).alias("currency_length_dense_rank"),
    )
)

with_windows_price_tiers = (
    with_windows
    .select(
        "*",
        F.percent_rank().over(w3).alias("currency_length_dense_rank_percent_rank"),
        F.percent_rank().over(w_grp).alias("currency_length_percent_rank_2"),
        F.dense_rank().over(w5).alias("currency_length_percent_rank_dense_rank"),
        F.avg("val").over(w_part).alias("effective_from_year_avg_val"),
        F.rank().over(w7).alias("val_rank"),
    )
)

totals = (
    with_windows_price_tiers
    .groupBy(F.date_trunc("quarter", "effective_from").alias("quarter_start"))
    .agg(
        F.percentile_approx("currency_length_dense_rank_percent_rank", 0.95).alias("currency_length_dense_rank_percent_rank_p95"),
        F.percentile_approx("currency_length_percent_rank_2", 0.9).alias("currency_length_percent_rank_2_p90"),
        F.percentile_approx("val_rank", [0.25, 0.5, 0.75]).alias("val_rank_quartiles"),
    )
)

result = (
    totals
    .select("quarter_start", "currency_length_percent_rank_2_p90", "val_rank_quartiles")
    .orderBy(F.col("quarter_start").desc())
)
