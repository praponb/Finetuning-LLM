from pyspark.sql import functions as F

lines = (
    df_price_tiers
    .select(
        "id",
        "effective_from",
        "currency",
        F.explode("tier_limits").alias("tier_value"),
    )
)

result = (
    lines
    .groupBy(F.date_trunc("quarter", "effective_from").alias("quarter_start"))
    .agg(
        F.sum(F.when(F.col("currency") == "EUR", F.col("tier_value")).otherwise(0)).alias("eur_tier_value"),
        F.round(F.avg("tier_value"), 2).alias("avg_tier_value"),
        F.count(F.when(F.col("currency") == "USD", 1)).alias("usd_rows"),
        F.count(F.when(F.col("currency") == "EUR", 1)).alias("eur_rows"),
        F.count_if(F.col("currency") == "THB").alias("thb_events"),
        F.count_if(F.col("currency") == "EUR").alias("eur_events"),
    )
    .filter(F.col("eur_rows") < 1000)
    .orderBy("usd_rows")
)
