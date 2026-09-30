from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("entry_price", "id")
w2 = Window.orderBy(F.col("entry_price").desc(), "id")
w3 = Window.orderBy(F.col("entry_price").asc_nulls_last())

flattened_attrs = (
    df_price_tiers
    .select(
        "id",
        "effective_from",
        "tier_limits",
        "tier_prices",
        "discounts",
        F.element_at("tier_prices", 1).alias("entry_price"),
        F.transform("tier_prices", lambda p, i: F.round(p * F.col("tier_limits")[i], 2)).alias("tier_revenue_caps"),
        F.slice("tier_limits", 1, 2).alias("first_two_limits"),
    )
)

result = (
    flattened_attrs
    .select(
        "id",
        "effective_from",
        "entry_price",
        F.ntile(10).over(w).alias("entry_price_bucket"),
        F.ntile(4).over(w2).alias("entry_price_bucket_2"),
        F.rank().over(w3).alias("entry_price_rank"),
    )
)
