from pyspark.sql import functions as F

exploded = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        F.explode_outer("tier_prices").alias("tier_value"),
    )
)

result = (
    exploded
    .groupBy(
        F.when(F.col("tier_value") >= 79, "PREMIUM").otherwise("STANDARD").alias("tier_value_tier"),
    )
    .agg(
        F.count_if(F.col("currency") == "EUR").alias("eur_events"),
        F.count(F.when(F.col("currency") == "USD", 1)).alias("usd_rows"),
    )
)
