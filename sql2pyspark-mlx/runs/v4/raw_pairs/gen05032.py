from pyspark.sql import functions as F

lines = (
    df_price_tiers
    .select(
        "id",
        "effective_from",
        "currency",
        F.explode_outer("tier_prices").alias("tier_value"),
    )
)

result = (
    lines
    .withColumn("tier_value_tier", F.when(F.col("tier_value") >= 71, "HIGH").otherwise("LOW"))
    .cube("currency", "tier_value_tier")
    .agg(
        F.count("*").alias("event_count"),
        F.sum("tier_value").alias("total_tier_value"),
        F.count(F.when(F.col("currency") == "EUR", 1)).alias("eur_rows"),
        F.sort_array(F.collect_set("currency")).alias("currency_list"),
        F.sort_array(F.collect_set("currency")).alias("currency_list_2"),
        F.grouping("currency").alias("gid"),
    )
    .filter(F.col("event_count") >= 1)
)
