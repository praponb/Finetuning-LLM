from pyspark.sql import functions as F

flattened = (
    df_price_tiers
    .select("id", "effective_from", "currency", F.explode("tier_limits").alias("val"))
)

result = (
    flattened
    .select(
        "id",
        "effective_from",
        "currency",
        "val",
        F.when(F.col("currency").rlike("^(THB|EUR)$"), "GROUP_B").when(F.col("currency").rlike("^(USD)$"), "OTHER_ZONE").otherwise("EXTENDED").alias("currency_zone"),
    )
)
