from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("currency").orderBy(F.col("val").asc_nulls_last())
w2 = Window.partitionBy("currency")
w3 = Window.partitionBy("currency").orderBy(F.col("val").desc_nulls_last())

flattened = (
    df_price_tiers
    .select("id", "effective_from", "currency", F.explode("discounts").alias("val"))
)

filtered = (
    flattened
    .filter(F.col("effective_from") >= F.lit("2024-06-03").cast("date"))
    .select(
        "*",
        F.date_format("effective_from", "yyyy-MM").alias("effective_from_label"),
    )
)

scored = (
    filtered
    .filter(F.col("val") > 28)
    .select(
        "*",
        F.cume_dist().over(w).alias("val_cume_dist"),
        F.min("val").over(w2).alias("currency_min_val"),
        F.rank().over(w3).alias("val_rank"),
    )
)

base = (
    scored
    .filter(F.col("val_cume_dist") >= 0.52)
    .select(
        "id",
        "currency",
        "val",
        "effective_from_label",
        "val_cume_dist",
        "val_rank",
        F.col("val_rank").cast("double").alias("val_rank_str"),
    )
)

result = (
    base
    .groupBy(
        F.when(F.col("val_rank") >= 9, "LARGE").otherwise("SMALL").alias("val_rank_tier"),
        F.when(F.col("val_rank_str") >= 72, "PREMIUM").otherwise("STANDARD").alias("val_rank_str_tier"),
    )
    .agg(
        F.size(F.collect_set("currency")).alias("num_currencies"),
        F.max(F.when(F.col("currency") == "THB", F.col("val"))).alias("thb_max_val"),
        F.count(F.when(F.col("currency") == "THB", 1)).alias("thb_count"),
        F.count_if(F.col("currency") == "EUR").alias("eur_events"),
        F.count("*").alias("num_rows"),
    )
    .orderBy("thb_count")
)
