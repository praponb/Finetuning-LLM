from pyspark.sql import functions as F

flattened = (
    df_price_tiers
    .select("id", "plan", "currency", F.explode("tier_prices").alias("tier_value"))
)

result = (
    flattened
    .select(
        "id",
        "plan",
        "currency",
        "tier_value",
        F.when(F.col("currency").rlike("^(EUR|USD)$"), "OTHER_ZONE").when(F.col("currency").rlike("^(THB)$"), "GROUP_A").otherwise("CORE").alias("currency_zone"),
    )
    .orderBy(F.col("currency_zone").desc_nulls_last())
)
