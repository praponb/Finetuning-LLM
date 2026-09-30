from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("currency").orderBy("avg_tier_price", "id").rowsBetween(-6, -1)
w_hist = Window.partitionBy("currency").orderBy("avg_tier_price", "id").rowsBetween(-3, -1)

parsed = (
    df_price_tiers
    .select(
        "id",
        "currency",
        "discounts",
        F.zip_with("tier_prices", "discounts", lambda p, d: F.round(p * (1 - d), 2)).alias("net_prices"),
        F.transform("tier_prices", lambda p, i: F.round(p * F.col("tier_limits")[i], 2)).alias("tier_revenue_caps"),
        F.aggregate("tier_prices", F.lit(0.0), lambda acc, p: acc + p, lambda acc: F.round(acc / F.size("tier_prices"), 2)).alias("avg_tier_price"),
        F.array_min("tier_prices").alias("lowest_price"),
    )
)

result = (
    parsed
    .select(
        "id",
        "currency",
        "avg_tier_price",
        F.avg("avg_tier_price").over(w_part).alias("avg_tier_price_avg_6_prior"),
        F.avg("avg_tier_price").over(w_hist).alias("avg_tier_price_avg_3_prior"),
    )
)
