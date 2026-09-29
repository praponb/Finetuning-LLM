from pyspark.sql import functions as F

flattened = (
    df_price_tiers
    .select("id", "effective_from", "currency", F.explode("tier_limits").alias("val"))
)

result = (
    flattened
    .groupBy(
        F.when(F.col("val") >= 88, "PREMIUM").otherwise("STANDARD").alias("val_tier"),
        "currency",
    )
    .agg(
        F.round(F.sum("val") / F.nullif(F.count(F.when(F.col("currency") == "THB", 1)), F.lit(0)), 2).alias("val_per_thb"),
        F.count(F.when(F.col("currency") == "USD", 1)).alias("usd_count"),
        F.count("*").alias("num_rows"),
        F.round(F.sum("val") / F.nullif(F.count(F.when(F.col("currency") == "USD", 1)), F.lit(0)), 2).alias("val_per_usd"),
        F.countDistinct("currency").alias("distinct_currencies"),
        F.round(F.sum("val") / F.nullif(F.count(F.when(F.col("currency") == "EUR", 1)), F.lit(0)), 2).alias("val_per_eur"),
    )
    .orderBy(F.col("num_rows").asc_nulls_last())
)
