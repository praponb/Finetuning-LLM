from pyspark.sql import functions as F

order_items_by_product = (
    df_order_items
    .filter(F.col("product_id").isNull())
    .groupBy("product_id")
    .agg((F.max("unit_price") - F.min("unit_price")).alias("unit_price_range"))
)

result = (
    df_products.alias("p")
    .join(order_items_by_product.alias("t"), F.col("p.id") == F.col("t.product_id"), "left")
    .select(F.col("p.id"), F.col("p.launch_date"), F.col("t.unit_price_range"))
)
