from pyspark.sql import functions as F

order_items_by_product = (
    df_order_items
    .filter(F.col("unit_price") >= 210)
    .groupBy("product_id")
    .agg(
        F.avg("quantity").alias("avg_quantity"),
        F.count("quantity").alias("quantity_count"),
    )
)

result = (
    df_products.alias("products")
    .join(order_items_by_product.alias("t"), F.col("products.id") == F.col("t.product_id"), "left")
    .select(
        F.col("products.launch_date"),
        F.col("t.avg_quantity"),
        F.col("t.quantity_count"),
    )
    .orderBy(F.col("avg_quantity").desc())
)
