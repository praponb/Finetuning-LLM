from pyspark.sql import functions as F

valid_order_items = (
    df_order_items
    .filter(F.col("order_id").isin([3, 13, 32]) & F.col("unit_price").between(280, 390))
)

order_items_stats = (
    valid_order_items
    .groupBy("product_id")
    .agg((F.max("unit_price") - F.min("unit_price")).alias("unit_price_range"))
)

result = (
    order_items_stats
    .filter(F.col("unit_price_range") >= 54)
    .select("product_id", "unit_price_range")
)
