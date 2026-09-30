from pyspark.sql import functions as F

flattened = (
    df_price_tiers
    .select("id", "currency", F.explode_outer("discounts").alias("element"))
)

result = (
    flattened
    .select(
        "id",
        "currency",
        F.when(F.col("currency").rlike("^(THB|EUR)$"), "PRIORITY").when(F.col("currency").rlike("^(USD)$"), "OTHER_ZONE").otherwise("GROUP_A").alias("currency_zone"),
    )
    .orderBy("id")
)
