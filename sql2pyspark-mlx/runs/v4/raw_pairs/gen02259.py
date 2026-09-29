from pyspark.sql import functions as F

exploded = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        F.explode("tier_prices").alias("element"),
    )
)

result = (
    exploded
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        "element",
        F.when(F.col("plan").rlike("^(pro|basic)$"), "PRIORITY").otherwise("OTHER_ZONE").alias("plan_zone"),
    )
)
