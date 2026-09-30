from pyspark.sql import functions as F

lines = (
    df_price_tiers
    .select("id", "currency", F.explode_outer("tier_prices").alias("tier_value"))
)

result = (
    lines
    .select(
        "id",
        "currency",
        "tier_value",
        F.when(F.col("currency").rlike("^(USD|EUR)$"), "CORE").when(F.col("currency").rlike("^(THB)$"), "EXTENDED").otherwise("OTHER_ZONE").alias("currency_zone"),
    )
    .orderBy(F.col("currency_zone").desc_nulls_first())
)
