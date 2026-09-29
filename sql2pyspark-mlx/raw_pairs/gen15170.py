from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("currency")
w2 = Window.orderBy(F.col("val").desc_nulls_last())
w_recent = Window.partitionBy("currency").orderBy(F.col("val_int").asc_nulls_last())
w4 = Window.partitionBy("currency").orderBy(F.col("val").asc_nulls_last())
w5 = Window.partitionBy("currency").orderBy(F.col("val").desc())
w6 = Window.orderBy(F.col("currency_min_val_int").desc_nulls_last())
w7 = Window.orderBy(F.col("val_cume_dist").desc_nulls_first())
w8 = Window.orderBy(F.col("val_int").desc())

exploded = df_price_tiers.select("id", "currency", F.explode("tier_prices").alias("val"))

valid = (
    exploded
    .filter(F.col("currency") == "EUR")
    .select("*", F.col("val").cast("int").alias("val_int"))
)

scored = (
    valid
    .select(
        "val",
        "val_int",
        F.min("val_int").over(w).alias("currency_min_val_int"),
        F.cume_dist().over(w2).alias("val_cume_dist"),
        F.dense_rank().over(w_recent).alias("val_int_dense_rank"),
        F.dense_rank().over(w4).alias("val_dense_rank"),
        F.cume_dist().over(w5).alias("val_cume_dist_2"),
    )
)

base = (
    scored
    .filter(F.col("val") <= 51)
    .select(
        "val_int",
        "currency_min_val_int",
        "val_cume_dist",
        "val_int_dense_rank",
        "val_cume_dist_2",
        (F.col("val") / F.nullif("val_cume_dist_2", F.lit(0))).alias("val_per_val_cume_dist_2"),
    )
)

scored_price_tiers = (
    base
    .select(
        "*",
        F.percent_rank().over(w6).alias("currency_min_val_int_percent_rank"),
        F.percent_rank().over(w7).alias("val_cume_dist_percent_rank"),
        F.percent_rank().over(w8).alias("val_int_percent_rank"),
    )
)

result = (
    scored_price_tiers
    .select(
        "val_int_dense_rank",
        "val_cume_dist_2",
        "val_per_val_cume_dist_2",
        "val_cume_dist_percent_rank",
        "val_int_percent_rank",
    )
    .orderBy(F.col("val_per_val_cume_dist_2").desc_nulls_first())
)
