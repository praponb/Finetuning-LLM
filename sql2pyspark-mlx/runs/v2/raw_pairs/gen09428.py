from pyspark.sql import functions as F

recent_order_items = df_order_items.filter(F.col("order_id").isin([6, 32]) & (F.col("unit_price") < 220))

agg = (
    recent_order_items
    .groupBy("product_id", "order_id")
    .agg(
        F.round(F.stddev("unit_price"), 2).alias("stddev_unit_price"),
        F.sum((F.col("unit_price") * F.col("quantity"))).alias("revenue"),
    )
)

result = (
    agg
    .filter(F.col("revenue") >= 73)
    .select("product_id", "stddev_unit_price", "revenue")
)
