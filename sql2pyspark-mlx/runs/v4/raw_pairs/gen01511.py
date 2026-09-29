from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("plan").orderBy("lowest_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("plan")
w3 = Window.partitionBy("plan").orderBy("entry_price", "id")

unpacked = (
    df_price_tiers
    .filter(F.col("plan") != "basic")
    .select(
        "id",
        "plan",
        "effective_from",
        F.element_at("tier_prices", 1).alias("entry_price"),
        F.array_min("tier_prices").alias("lowest_price"),
        F.transform("tier_prices", lambda p, i: F.round(p * F.col("tier_limits")[i], 2)).alias("tier_revenue_caps"),
        F.slice("tier_limits", 1, 2).alias("first_two_limits"),
        F.zip_with("tier_prices", "discounts", lambda p, d: F.round(p * (1 - d), 2)).alias("net_prices"),
    )
)

result = (
    unpacked
    .withColumn("cumulative_lowest_price", F.sum("lowest_price").over(w_recent))
    .withColumn("last_known_entry_price", F.last("entry_price", ignorenulls=True).over(w_recent))
    .withColumn("plan_min_entry_price", F.min("entry_price").over(w2))
    .withColumn("entry_price_bucket", F.ntile(10).over(w3))
)
