from pyspark.sql import functions as F

order_items_by_product = (
    df_order_items
    .filter(F.col("unit_price") < 220)
    .groupBy("product_id")
    .agg(
        F.min("unit_price").alias("min_unit_price"),
        F.sum("quantity").alias("total_quantity"),
    )
)

result = (
    df_products.alias("products")
    .join(order_items_by_product.alias("t"), F.col("products.id") == F.col("t.product_id"), "inner")
    .select(
        F.col("products.id"),
        F.col("products.price"),
        F.col("t.min_unit_price"),
        F.col("t.total_quantity"),
    )
)
