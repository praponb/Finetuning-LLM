from pyspark.sql import functions as F

order_items_by_product = (
    df_order_items
    .groupBy("product_id")
    .agg(F.avg("quantity").alias("avg_quantity"))
)

result = (
    df_products.alias("products")
    .join(order_items_by_product.alias("order_items_by_product"), F.col("products.id") == F.col("order_items_by_product.product_id"), "inner")
    .select(F.col("products.brand"), F.col("order_items_by_product.avg_quantity"))
    .orderBy("avg_quantity")
)
