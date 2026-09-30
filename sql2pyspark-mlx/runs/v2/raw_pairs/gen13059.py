from pyspark.sql import functions as F

product_stats = (
    df_order_items
    .filter(F.col("unit_price") >= 400)
    .groupBy("product_id")
    .agg(F.count("unit_price").alias("unit_price_count"))
)

result = (
    df_products.alias("products")
    .join(product_stats.alias("product_stats"), F.col("products.id") == F.col("product_stats.product_id"), "left")
    .select(
        F.col("products.brand"),
        F.col("products.launch_date"),
        F.col("product_stats.unit_price_count"),
    )
)
