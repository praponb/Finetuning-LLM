from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan")
w2 = Window.orderBy("avg_tier_price", "id")
w3 = Window.partitionBy("plan").orderBy("effective_from", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("plan").orderBy("avg_tier_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

flattened_attrs = (
    df_price_tiers
    .filter(F.col("plan") == "starter")
    .select(
        "id",
        "plan",
        "effective_from",
        "tier_limits",
        F.slice("tier_limits", 1, 2).alias("first_two_limits"),
        F.aggregate("tier_prices", F.lit(0.0), lambda acc, p: acc + p, lambda acc: F.round(acc / F.size("tier_prices"), 2)).alias("avg_tier_price"),
        F.array_min("tier_prices").alias("lowest_price"),
    )
)

result = (
    flattened_attrs
    .select(
        "*",
        F.avg("avg_tier_price").over(w).alias("plan_avg_avg_tier_price"),
        F.lead("avg_tier_price", 1).over(w2).alias("next_avg_tier_price"),
        F.sum("avg_tier_price").over(w3).alias("cumulative_avg_tier_price"),
        F.sum("lowest_price").over(w4).alias("running_lowest_price"),
        F.sum("avg_tier_price").over(w3).alias("running_avg_tier_price"),
    )
)
