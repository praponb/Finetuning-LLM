from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("avg_tier_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("currency").orderBy("avg_tier_price", "id")
w3 = Window.partitionBy("currency").orderBy("avg_tier_price", "id").rowsBetween(-2, Window.currentRow)

parsed = (
    df_price_tiers
    .filter(F.col("currency").isin(["THB", "EUR", "USD"]))
    .select(
        "id",
        "currency",
        "tier_limits",
        "tier_prices",
        F.transform("tier_prices", lambda p, i: F.round(p * F.col("tier_limits")[i], 2)).alias("tier_revenue_caps"),
        F.aggregate("tier_prices", F.lit(0.0), lambda acc, p: acc + p, lambda acc: F.round(acc / F.size("tier_prices"), 2)).alias("avg_tier_price"),
    )
)

result = (
    parsed
    .filter(F.col("currency") == "USD")
    .select(
        "id",
        "avg_tier_price",
        F.first("avg_tier_price", ignorenulls=True).over(w).alias("first_known_avg_tier_price"),
        F.sum("avg_tier_price").over(w).alias("running_avg_tier_price"),
        F.lead("avg_tier_price", 1, 0).over(w2).alias("next_avg_tier_price"),
        F.avg("avg_tier_price").over(w3).alias("avg_tier_price_avg_2"),
    )
)
