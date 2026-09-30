from pyspark.sql import functions as F

order_items_by_product = (
    df_order_items
    .groupBy("product_id")
    .agg(
        F.min("quantity").alias("min_quantity"),
        F.sum((F.col("quantity") * F.col("unit_price"))).alias("revenue"),
    )
)

result = (
    df_products.alias("products")
    .join(order_items_by_product.alias("order_items_by_product"), F.col("products.id") == F.col("order_items_by_product.product_id"), "inner")
    .select(
        F.col("products.stock_qty"),
        F.col("order_items_by_product.min_quantity"),
        F.col("order_items_by_product.revenue"),
    )
    .orderBy(F.col("min_quantity").desc())
)
