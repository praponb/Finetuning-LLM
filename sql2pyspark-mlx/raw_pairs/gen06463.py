from pyspark.sql import functions as F

flattened = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        F.explode("tier_prices").alias("tier_value"),
    )
)

result = (
    flattened
    .select(
        "id",
        "effective_from",
        "currency",
        "tier_value",
        F.when(F.col("currency").rlike("^(USD|THB)$"), "GROUP_B").when(F.col("currency").rlike("^(EUR)$"), "PRIORITY").otherwise("OTHER_ZONE").alias("currency_zone"),
    )
    .orderBy(F.col("currency_zone").desc_nulls_last())
)
