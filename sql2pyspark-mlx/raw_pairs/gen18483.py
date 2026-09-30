from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("avg_tier_price", "id")
w_ordered = Window.partitionBy(F.year("effective_from")).orderBy("effective_from", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy(F.date_trunc("MONTH", "effective_from")).orderBy("effective_from", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

unpacked = (
    df_price_tiers
    .select(
        "id",
        "effective_from",
        "tier_prices",
        "discounts",
        F.aggregate("tier_prices", F.lit(0.0), lambda acc, p: acc + p, lambda acc: F.round(acc / F.size("tier_prices"), 2)).alias("avg_tier_price"),
        F.element_at("tier_prices", 1).alias("entry_price"),
    )
)

result = (
    unpacked
    .withColumn("prev_entry_price", F.lag("entry_price", 2).over(w))
    .withColumn("first_known_entry_price", F.first("entry_price", ignorenulls=True).over(w_ordered))
    .withColumn("cumulative_entry_price", F.sum("entry_price").over(w3))
)
