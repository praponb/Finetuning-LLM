from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("avg_tier_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("currency").orderBy(F.col("avg_tier_price").desc(), "id")

unpacked = (
    df_price_tiers
    .select(
        "id",
        "currency",
        "tier_limits",
        "tier_prices",
        F.aggregate("tier_prices", F.lit(0.0), lambda acc, p: acc + p, lambda acc: F.round(acc / F.size("tier_prices"), 2)).alias("avg_tier_price"),
        F.transform("tier_prices", lambda p, i: F.round(p * F.col("tier_limits")[i], 2)).alias("tier_revenue_caps"),
    )
)

result = (
    unpacked
    .select(
        "id",
        "currency",
        "avg_tier_price",
        F.first("avg_tier_price", ignorenulls=True).over(w).alias("first_known_avg_tier_price"),
        F.sum("avg_tier_price").over(w).alias("cumulative_avg_tier_price"),
        F.row_number().over(w2).alias("event_seq"),
    )
)
