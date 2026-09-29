from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("currency")
w2 = Window.partitionBy("currency").orderBy(F.col("tier_value").desc_nulls_last())
w3 = Window.partitionBy("currency").orderBy(F.col("tier_value").asc_nulls_first())
w4 = Window.orderBy(F.col("currency_avg_tier_value").desc_nulls_last())
w5 = Window.orderBy(F.col("currency_min_tier_value").desc_nulls_first())

lines = (
    df_price_tiers
    .select("id", "currency", F.explode("tier_limits").alias("tier_value"))
)

filtered = (
    lines
    .filter(F.col("currency") == "USD")
    .withColumn("currency_lower", F.lower("currency"))
)

scored = (
    filtered
    .select(
        "tier_value",
        "currency_lower",
        F.count("tier_value").over(w).alias("currency_count_tier_value"),
        F.cume_dist().over(w2).alias("tier_value_cume_dist"),
        F.min("tier_value").over(w).alias("currency_min_tier_value"),
        F.avg("tier_value").over(w).alias("currency_avg_tier_value"),
        F.rank().over(w3).alias("tier_value_rank"),
    )
)

filtered_price_tiers = (
    scored
    .filter(F.col("currency_min_tier_value").between(26, 71))
    .select(
        "currency_lower",
        "currency_count_tier_value",
        "currency_min_tier_value",
        "currency_avg_tier_value",
        "tier_value_rank",
        F.least("currency_count_tier_value", "currency_min_tier_value").alias("least_currency_count_tier_value_currency_min_tier_value"),
    )
)

scored_price_tiers = (
    filtered_price_tiers
    .select(
        "currency_count_tier_value",
        "least_currency_count_tier_value_currency_min_tier_value",
        F.rank().over(w4).alias("currency_avg_tier_value_rank"),
        F.cume_dist().over(w5).alias("currency_min_tier_value_cume_dist"),
    )
)

result = (
    scored_price_tiers
    .select("currency_min_tier_value_cume_dist")
    .orderBy(F.col("currency_min_tier_value_cume_dist").asc_nulls_first())
)
