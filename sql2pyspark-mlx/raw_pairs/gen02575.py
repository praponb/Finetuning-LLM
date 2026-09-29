from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("currency").orderBy(F.col("val").asc_nulls_first())
w2 = Window.partitionBy("currency")
w3 = Window.partitionBy("currency").orderBy(F.col("val").desc_nulls_last())
w4 = Window.orderBy("currency_avg_val")

flattened = (
    df_price_tiers
    .select(
        "id",
        "effective_from",
        "currency",
        F.explode_outer("tier_prices").alias("val"),
    )
)

base = (
    flattened
    .filter(F.col("effective_from") > F.lit("2024-05-17").cast("date"))
    .select(
        "*",
        F.trim("currency").alias("clean_currency"),
        F.date_add("effective_from", 66).alias("effective_from_plus_66d"),
    )
)

enriched = (
    base
    .select(
        "*",
        F.cume_dist().over(w_grp).alias("val_cume_dist"),
        F.cume_dist().over(w_grp).alias("val_cume_dist_2"),
        F.avg("val").over(w2).alias("currency_avg_val"),
        F.dense_rank().over(w3).alias("val_dense_rank"),
    )
)

with_windows = (
    enriched
    .select(
        "currency",
        "clean_currency",
        "val_cume_dist_2",
        "currency_avg_val",
        "val_dense_rank",
        F.cume_dist().over(w4).alias("currency_avg_val_cume_dist"),
        F.dense_rank().over(w3).alias("val_dense_rank_2"),
        F.max("currency_avg_val").over(w2).alias("currency_max_currency_avg_val"),
    )
)

cleaned = (
    with_windows
    .filter(F.col("val_dense_rank") >= 5)
    .select(
        "currency",
        "clean_currency",
        "currency_avg_val",
        "val_dense_rank",
        "val_dense_rank_2",
        "currency_max_currency_avg_val",
        F.round((F.col("currency_avg_val_cume_dist") / F.col("val_dense_rank_2")), 2).alias("currency_avg_val_cume_dist_per_val_dense_rank_2"),
        F.when(F.col("val_dense_rank_2") >= 9, "High").when(F.col("val_dense_rank_2") >= 4, "Medium").otherwise("Low").alias("val_dense_rank_2_band"),
    )
)

result = (
    cleaned
    .select(
        "currency_avg_val",
        "val_dense_rank",
        "val_dense_rank_2",
        "currency_max_currency_avg_val",
        "val_dense_rank_2_band",
    )
    .orderBy("val_dense_rank_2", F.col("val_dense_rank").desc())
)
