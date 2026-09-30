from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("tier_value").asc_nulls_first())
w2 = Window.partitionBy("currency")
w3 = Window.orderBy(F.col("tier_value_plus_tier_value_cume_dist").asc_nulls_last())

lines = (
    df_price_tiers
    .select("id", "currency", F.explode("tier_prices").alias("tier_value"))
)

filtered = (
    lines
    .filter(F.col("currency") == "THB")
    .withColumn("currency_lower", F.lower("currency"))
)

enriched = (
    filtered
    .select(
        "currency",
        "tier_value",
        F.cume_dist().over(w).alias("tier_value_cume_dist"),
        F.sum("tier_value").over(w2).alias("currency_sum_tier_value"),
    )
)

cleaned = (
    enriched
    .filter(F.col("currency_sum_tier_value") >= 317)
    .withColumn("tier_value_plus_tier_value_cume_dist", (F.col("tier_value") + F.col("tier_value_cume_dist")))
)

with_windows = (
    cleaned
    .select(
        "tier_value",
        "currency_sum_tier_value",
        F.percent_rank().over(w3).alias("tier_value_plus_tier_value_cume_dist_percent_rank"),
        F.sum("tier_value").over(w2).alias("currency_sum_tier_value_2"),
    )
)

result = (
    with_windows
    .select(
        "currency_sum_tier_value",
        "tier_value_plus_tier_value_cume_dist_percent_rank",
    )
)
