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
        F.when(F.col("tier_value") >= 64, "PREMIUM").otherwise("STANDARD").alias("tier_value_tier"),
    )
    .agg(
        F.round(F.avg("tier_value"), 2).alias("avg_tier_value"),
        F.max(F.when(F.col("currency") == "EUR", F.col("tier_value"))).alias("eur_max_tier_value"),
        F.max("tier_value").alias("max_tier_value"),
        F.sum(F.when(F.col("plan") == "pro", F.col("tier_value")).otherwise(0)).alias("pro_tier_value"),
        F.round(F.avg("tier_value"), 2).alias("avg_tier_value_2"),
        F.count("*").alias("num_rows"),
    )
)
