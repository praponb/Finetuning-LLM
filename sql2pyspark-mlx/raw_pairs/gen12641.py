from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("currency")

parsed = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "currency",
        "discounts",
        F.slice("tier_limits", 1, 2).alias("first_two_limits"),
        F.transform("tier_prices", lambda p, i: F.round(p * F.col("tier_limits")[i], 2)).alias("tier_revenue_caps"),
        F.element_at("tier_prices", 1).alias("entry_price"),
    )
)

result = (
    parsed
    .select(
        "id",
        "currency",
        "entry_price",
        F.avg("entry_price").over(w).alias("currency_avg_entry_price"),
        F.min("entry_price").over(w).alias("currency_min_entry_price"),
    )
)
