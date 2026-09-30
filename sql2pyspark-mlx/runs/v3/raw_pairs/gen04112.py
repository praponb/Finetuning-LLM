from pyspark.sql import functions as F

product_stats = (
    df_order_items
    .groupBy("product_id")
    .agg(F.count("*").alias("order_item_count"))
)

result = (
    df_products.alias("products")
    .join(product_stats.alias("product_stats"), F.col("products.id") == F.col("product_stats.product_id"), "inner")
    .filter(F.col("product_stats.order_item_count") >= 4)
    .select(F.col("products.price"), F.col("product_stats.order_item_count"))
    .orderBy("price")
)
