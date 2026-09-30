from pyspark.sql import functions as F

exploded = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        F.explode_outer("discounts").alias("tier_value"),
    )
)

result = (
    exploded
    .select("plan", "effective_from")
    .orderBy(F.col("effective_from").desc_nulls_last())
)
