from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("currency")
w2 = Window.orderBy(F.col("val").asc_nulls_first())
w3 = Window.partitionBy("currency", "plan").orderBy(F.col("val").desc_nulls_last())
w4 = Window.orderBy(F.col("val_rank").asc_nulls_first())

flattened = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        F.explode_outer("tier_prices").alias("val"),
    )
)

scored = (
    flattened
    .withColumn("currency_min_val", F.min("val").over(w))
    .withColumn("val_percent_rank", F.percent_rank().over(w2))
    .withColumn("val_rank", F.rank().over(w3))
)

base = (
    scored
    .filter(F.col("effective_from") >= F.lit("2024-05-11").cast("date"))
    .select(
        "id",
        "effective_from",
        "currency",
        "val",
        "currency_min_val",
        "val_percent_rank",
        "val_rank",
        F.coalesce("val_percent_rank", F.lit(0)).alias("val_percent_rank_filled"),
        F.when(F.col("val") >= 75, "High").when(F.col("val") >= 28, "Medium").otherwise("Low").alias("val_band"),
    )
)

with_windows = (
    base
    .withColumn("currency_min_val_percent_rank", F.min("val_percent_rank").over(w))
    .withColumn("val_rank_share", F.round((F.col("val_rank") / F.sum("val_rank").over(w)), 4))
    .withColumn("currency_count_val_percent_rank_filled", F.count("val_percent_rank_filled").over(w))
    .withColumn("val_rank_rank", F.rank().over(w4))
)

result = (
    with_windows
    .select(
        "*",
        F.max("currency_min_val_percent_rank").over(w).alias("currency_max_currency_min_val_percent_rank"),
        F.sum("val_percent_rank").over(w).alias("currency_sum_val_percent_rank"),
        F.round((F.col("val_rank_rank") / F.sum("val_rank_rank").over(w)), 4).alias("val_rank_rank_share"),
        F.stddev_samp("val_percent_rank_filled").over(w).alias("currency_stddev_samp_val_percent_rank_filled"),
    )
)
