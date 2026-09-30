from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("currency")
w2 = Window.partitionBy("currency").orderBy(F.col("val").asc_nulls_first())
w3 = Window.partitionBy(F.date_trunc("WEEK", "effective_from"))
w4 = Window.partitionBy(F.date_trunc("MONTH", "effective_from"), "currency").orderBy(F.col("is_eur").desc_nulls_first())

flattened = (
    df_price_tiers
    .select("id", "effective_from", "currency", F.explode("tier_limits").alias("val"))
)

filtered = (
    flattened
    .filter((F.col("effective_from") > F.lit("2024-02-13").cast("date")) & (F.col("val") > 34))
    .select(
        "*",
        F.round("val", 2).alias("val_rounded"),
        F.when(F.col("currency") == "EUR", 1).otherwise(0).alias("is_eur"),
    )
)

cleaned = (
    filtered
    .filter(F.col("effective_from") >= F.lit("2024-01-27").cast("date"))
    .select(
        "effective_from",
        "currency",
        "val",
        "val_rounded",
        "is_eur",
        (F.col("is_eur") / F.nullif("val_rounded", F.lit(0))).alias("is_eur_per_val_rounded"),
    )
)

scored = (
    cleaned
    .withColumn("currency_count_val_rounded", F.count("val_rounded").over(w))
    .withColumn("val_percent_rank", F.percent_rank().over(w2))
    .withColumn("effective_from_period_count_val_rounded", F.count("val_rounded").over(w3))
    .withColumn("is_eur_cume_dist", F.cume_dist().over(w4))
)

base = (
    scored
    .filter(F.col("effective_from") > F.to_date(F.lit("2024-03-31")))
    .select(
        "effective_from",
        "currency",
        "val",
        "val_rounded",
        "is_eur",
        "is_eur_per_val_rounded",
        "effective_from_period_count_val_rounded",
        F.when(F.col("is_eur_cume_dist") >= 0.7, "High").when(F.col("is_eur_cume_dist") >= 0.24, "Medium").otherwise("Low").alias("is_eur_cume_dist_band"),
    )
)

filtered_price_tiers = (
    base
    .filter(F.col("effective_from_period_count_val_rounded").between(11, 16))
    .select(
        "*",
        F.coalesce("effective_from_period_count_val_rounded", F.lit(0)).alias("effective_from_period_count_val_rounded_filled"),
    )
)

valid = (
    filtered_price_tiers
    .filter(F.col("effective_from") > F.lit("2024-02-03").cast("date"))
    .withColumn("currency_lower", F.lower("currency"))
    .withColumn("currency_title", F.initcap(F.trim("currency")))
)

result = (
    valid
    .select(
        "currency",
        "val",
        "is_eur",
        "is_eur_per_val_rounded",
        "is_eur_cume_dist_band",
        "effective_from_period_count_val_rounded_filled",
        "currency_lower",
        "currency_title",
        F.when(F.col("currency").rlike("^(USD|EUR)$"), "OTHER_ZONE").when(F.col("currency").rlike("^(THB)$"), "CORE").otherwise("GROUP_B").alias("currency_zone"),
    )
    .orderBy(F.col("currency_zone").desc())
)
