from pyspark.sql import functions as F

exploded = (
    df_price_tiers
    .select("id", "plan", "currency", F.explode_outer("tier_prices").alias("element"))
)

result = (
    exploded
    .select(
        "id",
        "plan",
        "currency",
        "element",
        F.when(F.col("currency").rlike("^(EUR|THB)$"), "PRIORITY").otherwise("CORE").alias("currency_zone"),
    )
)
