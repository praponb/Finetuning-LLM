from pyspark.sql import functions as F

order_items_by_product = (
    df_order_items
    .groupBy("product_id")
    .agg(
        F.count("*").alias("row_count"),
        F.count("*").alias("order_item_count"),
    )
)

result = (
    df_products.alias("products")
    .join(order_items_by_product.alias("order_items_by_product"), F.col("products.id") == F.col("order_items_by_product.product_id"), "left")
    .filter(F.col("order_items_by_product.order_item_count") >= 4)
    .select(
        F.col("products.id"),
        F.col("products.category"),
        F.col("order_items_by_product.row_count"),
        F.col("order_items_by_product.order_item_count"),
    )
    .orderBy(F.col("row_count").desc())
)
