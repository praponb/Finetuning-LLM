from pyspark.sql import functions as F

product_stats = (
    df_inventory
    .filter(F.col("product_id").isin([6, 7, 8, 10]))
    .groupBy("product_id")
    .agg(
        F.avg("quantity").alias("avg_quantity"),
        F.countDistinct("quantity").alias("unique_products"),
    )
)

result = (
    df_products.alias("products")
    .join(product_stats.alias("product_stats"), F.col("products.id") == F.col("product_stats.product_id"), "left")
    .select(
        F.col("products.id"),
        F.col("products.product_name"),
        F.col("products.launch_date"),
        F.col("product_stats.avg_quantity"),
        F.col("product_stats.unique_products"),
    )
)
