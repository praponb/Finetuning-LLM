from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy("avg_tier_price", "id")
w2 = Window.orderBy("avg_tier_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

unpacked = (
    df_price_tiers
    .filter(F.col("plan") == "pro")
    .select(
        "id",
        "plan",
        "discounts",
        F.aggregate("tier_prices", F.lit(0.0), lambda acc, p: acc + p, lambda acc: F.round(acc / F.size("tier_prices"), 2)).alias("avg_tier_price"),
        F.element_at("tier_prices", 1).alias("entry_price"),
        F.slice("tier_limits", 1, 2).alias("first_two_limits"),
        F.array_min("tier_prices").alias("lowest_price"),
    )
)

result = (
    unpacked
    .select(
        "id",
        "avg_tier_price",
        "entry_price",
        F.lead("lowest_price", 2).over(w).alias("next_lowest_price"),
        F.first("entry_price", ignorenulls=True).over(w2).alias("first_known_entry_price"),
    )
)
