from pyspark.sql import functions as F

order_items_by_product = (
    df_order_items
    .groupBy("product_id")
    .agg(
        F.sum((F.col("unit_price") * F.col("quantity"))).alias("revenue"),
        F.count("*").alias("total_count"),
    )
)

result = (
    df_products.alias("products")
    .join(order_items_by_product.alias("order_items_by_product"), F.col("products.id") == F.col("order_items_by_product.product_id"), "inner")
    .select(
        F.col("products.id"),
        F.col("products.supplier_id"),
        F.col("products.stock_qty"),
        F.col("order_items_by_product.revenue"),
        F.col("order_items_by_product.total_count"),
    )
    .orderBy("supplier_id")
)
