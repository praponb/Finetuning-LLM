from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("currency").orderBy(F.col("is_usd").asc_nulls_last())
w2 = Window.partitionBy("currency").orderBy(F.col("tier_value").asc_nulls_last())
w3 = Window.partitionBy("currency")
w4 = Window.partitionBy("currency").orderBy(F.col("is_usd").asc_nulls_first())
w5 = Window.orderBy(F.col("tier_value").asc_nulls_first())

lines = (
    df_price_tiers
    .select("id", "currency", F.explode("tier_limits").alias("tier_value"))
)

base = (
    lines
    .filter(F.col("currency") == "EUR")
    .select(
        "*",
        F.when(F.col("currency") == "USD", 1).otherwise(0).alias("is_usd"),
        F.round("tier_value", 0).alias("tier_value_rounded"),
    )
)

with_windows = (
    base
    .filter(F.col("tier_value_rounded") <= 30)
    .withColumn("is_usd_rank", F.rank().over(w_hist))
    .withColumn("tier_value_cume_dist", F.cume_dist().over(w2))
    .withColumn("currency_sum_tier_value_rounded", F.sum("tier_value_rounded").over(w3))
    .withColumn("is_usd_percent_rank", F.percent_rank().over(w4))
)

cleaned = (
    with_windows
    .filter(F.col("tier_value") == 31)
    .withColumn("tier_value_rounded_tier", F.when(F.col("tier_value_rounded") >= 72, "High").when(F.col("tier_value_rounded") >= 37, "Medium").otherwise("Low"))
    .withColumn("currency_prefix", F.substring("currency", 1, 4))
)

result = (
    cleaned
    .select(
        "id",
        "tier_value",
        "is_usd",
        "tier_value_rounded",
        "is_usd_rank",
        "tier_value_cume_dist",
        "is_usd_percent_rank",
        "tier_value_rounded_tier",
        F.rank().over(w5).alias("tier_value_rank"),
        F.count("tier_value_cume_dist").over(w3).alias("currency_count_tier_value_cume_dist"),
        F.avg("tier_value_cume_dist").over(w3).alias("currency_avg_tier_value_cume_dist"),
    )
)
