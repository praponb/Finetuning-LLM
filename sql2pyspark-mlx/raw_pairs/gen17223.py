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
    .cube(
        F.date_trunc("YEAR", "effective_from").alias("year_start"),
        F.date_trunc("MONTH", "effective_from").alias("activity_month"),
    )
    .agg(
        F.round(F.sum("tier_value") / F.nullif(F.count(F.when(F.col("currency") == "USD", 1)), F.lit(0)), 2).alias("tier_value_per_usd"),
        F.countDistinct("plan").alias("distinct_plans"),
        F.min("tier_value").alias("min_tier_value"),
    )
    .filter(F.col("distinct_plans") >= 1)
    .orderBy(F.col("tier_value_per_usd").desc())
)
