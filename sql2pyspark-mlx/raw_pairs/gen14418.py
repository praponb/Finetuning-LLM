from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("currency")
w2 = Window.partitionBy("currency").orderBy("avg_tier_price")

parsed = (
    df_price_tiers
    .filter(F.col("effective_from").between("2023-10-01", "2024-01-01"))
    .select(
        "id",
        "effective_from",
        "currency",
        "tier_limits",
        "discounts",
        F.transform("tier_prices", lambda p, i: F.round(p * F.col("tier_limits")[i], 2)).alias("tier_revenue_caps"),
        F.aggregate("tier_prices", F.lit(0.0), lambda acc, p: acc + p, lambda acc: F.round(acc / F.size("tier_prices"), 2)).alias("avg_tier_price"),
    )
)

result = (
    parsed
    .select(
        "id",
        "effective_from",
        F.stddev_samp("avg_tier_price").over(w).alias("currency_stddev_samp_avg_tier_price"),
        F.percent_rank().over(w2).alias("avg_tier_price_percent_rank"),
    )
)
