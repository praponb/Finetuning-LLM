from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("plan").orderBy("lowest_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ordered = Window.orderBy("avg_tier_price", "id")

unpacked = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        "tier_limits",
        "discounts",
        F.aggregate("tier_prices", F.lit(0.0), lambda acc, p: acc + p, lambda acc: F.round(acc / F.size("tier_prices"), 2)).alias("avg_tier_price"),
        F.array_min("tier_prices").alias("lowest_price"),
        F.element_at("tier_prices", 1).alias("entry_price"),
    )
)

result = (
    unpacked
    .select(
        "*",
        F.sum("entry_price").over(w_recent).alias("running_entry_price"),
        F.lead("avg_tier_price", 1).over(w_ordered).alias("next_avg_tier_price"),
    )
)
