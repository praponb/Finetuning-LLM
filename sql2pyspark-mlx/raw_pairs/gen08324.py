from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("plan").orderBy("lowest_price", "id")

parsed = (
    df_price_tiers
    .filter(F.col("plan").isin(["pro", "basic", "enterprise"]))
    .select(
        "id",
        "plan",
        F.transform("tier_prices", lambda p, i: F.round(p * F.col("tier_limits")[i], 2)).alias("tier_revenue_caps"),
        F.array_min("tier_prices").alias("lowest_price"),
        F.slice("tier_limits", 1, 2).alias("first_two_limits"),
        F.zip_with("tier_prices", "discounts", lambda p, d: F.round(p * (1 - d), 2)).alias("net_prices"),
        F.aggregate("tier_prices", F.lit(0.0), lambda acc, p: acc + p, lambda acc: F.round(acc / F.size("tier_prices"), 2)).alias("avg_tier_price"),
    )
)

result = (
    parsed
    .select(
        "id",
        "avg_tier_price",
        F.lead("avg_tier_price").over(w_ordered).alias("next_avg_tier_price"),
        F.lag("lowest_price").over(w_ordered).alias("prev_lowest_price"),
        F.lead("lowest_price").over(w_ordered).alias("next_lowest_price"),
        F.row_number().over(w_ordered).alias("event_seq"),
    )
)
