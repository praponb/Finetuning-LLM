from pyspark.sql import functions as F

flattened = (
    df_price_tiers
    .select(
        "id",
        "effective_from",
        "currency",
        F.explode_outer("tier_prices").alias("tier_value"),
    )
)

result = (
    flattened
    .groupBy("currency", F.date_trunc("WEEK", "effective_from").alias("week_start"))
    .agg(
        F.countDistinct("currency").alias("distinct_currencies"),
        F.sum("tier_value").alias("total_tier_value"),
        F.sort_array(F.collect_set("currency")).alias("currency_list"),
        F.sum(F.when(F.col("currency") == "USD", F.col("tier_value")).otherwise(0)).alias("usd_tier_value"),
        F.count(F.when(F.col("currency") == "USD", 1)).alias("usd_rows"),
        F.count_if(F.col("currency") == "EUR").alias("eur_events"),
    )
)
