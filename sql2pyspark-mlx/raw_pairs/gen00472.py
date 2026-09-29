from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("entry_price", "id").rowsBetween(-2, Window.currentRow)
w2 = Window.partitionBy("plan").orderBy(F.col("entry_price").desc(), "id")
w3 = Window.partitionBy("plan").orderBy("entry_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

parsed = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "tier_limits",
        "tier_prices",
        "discounts",
        F.element_at("tier_prices", 1).alias("entry_price"),
        F.transform("tier_prices", lambda p, i: F.round(p * F.col("tier_limits")[i], 2)).alias("tier_revenue_caps"),
        F.slice("tier_limits", 1, 2).alias("first_two_limits"),
    )
)

result = (
    parsed
    .select(
        "id",
        "plan",
        "entry_price",
        F.avg("entry_price").over(w).alias("entry_price_avg_2"),
        F.row_number().over(w2).alias("event_seq"),
        F.sum("entry_price").over(w3).alias("running_entry_price"),
    )
)
