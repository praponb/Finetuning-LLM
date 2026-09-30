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
    .groupBy(
        F.date_trunc("WEEK", "effective_from").alias("week_start"),
        F.when(F.col("tier_value") >= 65, "PREMIUM").otherwise("STANDARD").alias("tier_value_tier"),
    )
    .agg(
        F.avg(F.when(F.col("currency") == "EUR", F.col("tier_value"))).alias("eur_avg_tier_value"),
        F.avg("tier_value").alias("avg_tier_value"),
    )
    .orderBy(F.col("avg_tier_value").desc_nulls_first())
)
