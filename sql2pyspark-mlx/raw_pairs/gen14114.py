from pyspark.sql import functions as F

product_stats = (
    df_order_items
    .groupBy("product_id")
    .agg(
        F.sum("unit_price").alias("total_unit_price"),
        F.countDistinct("quantity").alias("unique_orders"),
    )
)

result = (
    df_products.alias("products")
    .join(product_stats.alias("product_stats"), F.col("products.id") == F.col("product_stats.product_id"), "inner")
    .select(
        F.col("products.id"),
        F.col("products.cost"),
        F.col("products.stock_qty"),
        F.col("product_stats.total_unit_price"),
        F.col("product_stats.unique_orders"),
    )
)
