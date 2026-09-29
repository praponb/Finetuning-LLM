from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("currency").orderBy("entry_price", "id").rowsBetween(-3, Window.currentRow)

parsed = (
    df_price_tiers
    .select(
        "id",
        "currency",
        "tier_limits",
        "discounts",
        F.transform("tier_prices", lambda p, i: F.round(p * F.col("tier_limits")[i], 2)).alias("tier_revenue_caps"),
        F.element_at("tier_prices", 1).alias("entry_price"),
        F.zip_with("tier_prices", "discounts", lambda p, d: F.round(p * (1 - d), 2)).alias("net_prices"),
        F.slice("tier_limits", 1, 2).alias("first_two_limits"),
    )
)

result = (
    parsed
    .select(
        "id",
        "currency",
        "entry_price",
        F.min("entry_price").over(w).alias("last4_min_entry_price"),
        F.max("entry_price").over(w).alias("last4_max_entry_price"),
        F.count("entry_price").over(w).alias("last4_count_entry_price"),
        F.avg("entry_price").over(w).alias("last4_avg_entry_price"),
    )
)
