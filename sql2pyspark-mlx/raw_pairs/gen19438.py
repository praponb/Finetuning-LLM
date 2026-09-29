from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("plan").orderBy(F.col("lowest_price").desc())
w2 = Window.partitionBy(F.month("effective_from"), "plan").orderBy("lowest_price", "id")
w_part = Window.partitionBy(F.year("effective_from"))

flattened_attrs = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "tier_limits",
        "tier_prices",
        F.element_at("tier_prices", 1).alias("entry_price"),
        F.transform("tier_prices", lambda p, i: F.round(p * F.col("tier_limits")[i], 2)).alias("tier_revenue_caps"),
        F.zip_with("tier_prices", "discounts", lambda p, d: F.round(p * (1 - d), 2)).alias("net_prices"),
        F.array_min("tier_prices").alias("lowest_price"),
        F.aggregate("tier_prices", F.lit(0.0), lambda acc, p: acc + p, lambda acc: F.round(acc / F.size("tier_prices"), 2)).alias("avg_tier_price"),
    )
)

result = (
    flattened_attrs
    .filter(F.col("plan") != "starter")
    .select(
        "id",
        "plan",
        F.cume_dist().over(w_ordered).alias("lowest_price_cume_dist"),
        F.lag("entry_price", 2).over(w2).alias("prev_entry_price"),
        F.count("avg_tier_price").over(w_part).alias("effective_from_year_count_avg_tier_price"),
    )
)
