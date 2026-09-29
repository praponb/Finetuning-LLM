from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.month("effective_from"), "currency").orderBy(F.col("val").desc_nulls_last())
w2 = Window.partitionBy("currency")

lines = (
    df_price_tiers
    .select("id", "effective_from", "currency", F.explode("discounts").alias("val"))
)

filtered = (
    lines
    .filter(F.col("effective_from") >= "2025-07-01")
    .select(
        "*",
        F.date_format("effective_from", "yyyy-MM").alias("effective_from_label"),
    )
)

with_windows = (
    filtered
    .select(
        "id",
        "effective_from",
        "val",
        F.rank().over(w).alias("val_rank"),
        F.max("val").over(w2).alias("currency_max_val"),
        F.sum("val").over(w2).alias("currency_sum_val"),
    )
)

filtered_price_tiers = (
    with_windows
    .filter((F.col("effective_from") > F.lit("2024-05-06").cast("date")) & F.col("currency_max_val").between(58, 63))
    .select(
        "id",
        "effective_from",
        "val",
        "val_rank",
        "currency_max_val",
        "currency_sum_val",
        F.when(F.col("currency_max_val") >= 73, "High").when(F.col("currency_max_val") >= 20, "Medium").otherwise("Low").alias("currency_max_val_level"),
        (F.col("val_rank") / F.nullif("currency_max_val", F.lit(0))).alias("val_rank_per_currency_max_val"),
    )
)

result = (
    filtered_price_tiers
    .select(
        "id",
        "effective_from",
        "val",
        "currency_max_val",
        "currency_sum_val",
        "currency_max_val_level",
        "val_rank_per_currency_max_val",
    )
    .orderBy(F.col("val_rank_per_currency_max_val").desc())
)
