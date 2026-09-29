from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("currency").orderBy("lowest_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("currency").orderBy("lowest_price", "id")
w3 = Window.partitionBy("currency").orderBy("entry_price", "id").rowsBetween(-5, Window.currentRow)

unpacked = (
    df_price_tiers
    .select(
        "id",
        "currency",
        "discounts",
        F.transform("tier_prices", lambda p, i: F.round(p * F.col("tier_limits")[i], 2)).alias("tier_revenue_caps"),
        F.slice("tier_limits", 1, 2).alias("first_two_limits"),
        F.array_min("tier_prices").alias("lowest_price"),
        F.element_at("tier_prices", 1).alias("entry_price"),
    )
)

result = (
    unpacked
    .withColumn("first_known_lowest_price", F.first("lowest_price", ignorenulls=True).over(w))
    .withColumn("prev_entry_price", F.lag("entry_price", 1).over(w2))
    .withColumn("lowest_price_avg_5", F.avg("lowest_price").over(w3))
)
