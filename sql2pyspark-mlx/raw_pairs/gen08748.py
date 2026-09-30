from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("days_since_effective_from").desc_nulls_last())
w2 = Window.orderBy(F.col("val").asc_nulls_last())
w3 = Window.partitionBy(F.month("effective_from"))
w4 = Window.orderBy(F.col("effective_from_month_min_days_since_effective_from").desc_nulls_first())
w5 = Window.orderBy(F.col("val").desc_nulls_first())
w_ordered = Window.orderBy(F.col("val").desc_nulls_last())
w_ordered_2 = Window.orderBy("val_dense_rank")
w8 = Window.orderBy(F.col("val_dense_rank").desc())
w9 = Window.orderBy(F.col("val_dense_rank_2").desc())
w10 = Window.orderBy(F.col("val_dense_rank_percent_rank").desc_nulls_first())
w11 = Window.orderBy(F.col("days_since_effective_from_percent_rank").desc())
w12 = Window.orderBy(F.col("val_cume_dist").desc_nulls_last())
w13 = Window.orderBy("days_since_effective_from_percent_rank")
w14 = Window.orderBy("days_since_effective_from_percent_rank_dense_rank")
w_recent = Window.orderBy("days_since_effective_from")

lines = df_price_tiers.select("id", "effective_from", F.explode("discounts").alias("val"))

base = (
    lines
    .filter(F.col("val") >= 40)
    .withColumn("days_since_effective_from", F.datediff(F.current_date(), "effective_from"))
)

with_windows = (
    base
    .select(
        "*",
        F.percent_rank().over(w).alias("days_since_effective_from_percent_rank"),
        F.dense_rank().over(w2).alias("val_dense_rank"),
        F.min("days_since_effective_from").over(w3).alias("effective_from_month_min_days_since_effective_from"),
    )
)

with_windows_price_tiers = (
    with_windows
    .filter(F.col("days_since_effective_from") >= 51)
    .select(
        "id",
        "val",
        "days_since_effective_from",
        "days_since_effective_from_percent_rank",
        "val_dense_rank",
        "effective_from_month_min_days_since_effective_from",
        F.dense_rank().over(w4).alias("effective_from_month_min_days_since_effective_from_dense_rank"),
        F.dense_rank().over(w5).alias("val_dense_rank_2"),
        F.cume_dist().over(w_ordered).alias("val_cume_dist"),
        F.rank().over(w_ordered_2).alias("val_dense_rank_rank"),
        F.percent_rank().over(w8).alias("val_dense_rank_percent_rank"),
    )
)

with_windows_price_tiers_2 = (
    with_windows_price_tiers
    .select(
        "id",
        "days_since_effective_from",
        "days_since_effective_from_percent_rank",
        "val_dense_rank",
        "effective_from_month_min_days_since_effective_from_dense_rank",
        F.percent_rank().over(w9).alias("val_dense_rank_2_percent_rank"),
        F.percent_rank().over(w10).alias("val_dense_rank_percent_rank_percent_rank"),
        F.dense_rank().over(w11).alias("days_since_effective_from_percent_rank_dense_rank"),
        F.dense_rank().over(w12).alias("val_cume_dist_dense_rank"),
        F.percent_rank().over(w13).alias("days_since_effective_from_percent_rank_percent_rank"),
    )
)

with_windows_price_tiers_3 = (
    with_windows_price_tiers_2
    .select(
        "id",
        "days_since_effective_from",
        "val_dense_rank",
        "effective_from_month_min_days_since_effective_from_dense_rank",
        "val_cume_dist_dense_rank",
        F.cume_dist().over(w14).alias("days_since_effective_from_percent_rank_dense_rank_cume_dist"),
        F.dense_rank().over(w_recent).alias("days_since_effective_from_dense_rank"),
    )
)

result = (
    with_windows_price_tiers_3
    .groupBy(
        F.when(F.col("effective_from_month_min_days_since_effective_from_dense_rank") >= 7, "HIGH").otherwise("LOW").alias("effective_from_month_min_days_since_effective_from_dense_rank_tier"),
        F.when(F.col("days_since_effective_from_percent_rank_dense_rank_cume_dist") >= 0.8, "LARGE").otherwise("SMALL").alias("days_since_effective_from_percent_rank_dense_rank_cume_dist_tier"),
        F.when(F.col("val_dense_rank") >= 7, "LARGE").otherwise("SMALL").alias("val_dense_rank_tier"),
    )
    .agg(
        F.percentile_approx("days_since_effective_from_percent_rank_dense_rank_cume_dist", [0.25, 0.5, 0.75]).alias("days_since_effective_from_percent_rank_dense_rank_cume_dist_quartiles"),
        F.percentile_approx("days_since_effective_from_dense_rank", 0.95).alias("days_since_effective_from_dense_rank_p95"),
        F.count("*").alias("event_count"),
    )
    .orderBy(
        F.col("days_since_effective_from_percent_rank_dense_rank_cume_dist_tier").desc(),
    )
)
