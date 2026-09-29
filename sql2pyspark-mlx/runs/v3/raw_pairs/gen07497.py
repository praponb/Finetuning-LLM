from pyspark.sql import functions as F

order_items_by_product = (
    df_order_items
    .filter(F.col("unit_price") <= 380)
    .groupBy("product_id")
    .agg(F.count("quantity").alias("quantity_count"))
)

result = (
    df_products.alias("p")
    .join(order_items_by_product.alias("oibp"), F.col("p.id") == F.col("oibp.product_id"), "inner")
    .filter(F.col("oibp.quantity_count") > 8)
    .select(F.col("p.stock_qty"), F.col("p.cost"), F.col("oibp.quantity_count"))
)
