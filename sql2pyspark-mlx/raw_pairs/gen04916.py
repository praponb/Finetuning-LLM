from pyspark.sql import functions as F

product_summary = (
    df_order_items
    .filter(F.col("unit_price") > 210)
    .groupBy("product_id")
    .agg(F.count("quantity").alias("quantity_count"))
)

result = (
    df_products.alias("products")
    .join(product_summary.alias("product_summary"), F.col("products.id") == F.col("product_summary.product_id"), "inner")
    .select(
        F.col("products.id"),
        F.col("products.launch_date"),
        F.col("products.stock_qty"),
        F.col("product_summary.quantity_count"),
    )
)
