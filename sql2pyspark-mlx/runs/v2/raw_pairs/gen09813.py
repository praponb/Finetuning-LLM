from pyspark.sql import functions as F

product_summary = (
    df_inventory
    .groupBy("product_id")
    .agg(
        F.max("last_restock").alias("latest_last_restock"),
        F.count("quantity").alias("quantity_count"),
    )
)

result = (
    df_products.alias("products")
    .join(product_summary.alias("product_summary"), F.col("products.id") == F.col("product_summary.product_id"), "inner")
    .select(
        F.col("products.id"),
        F.col("products.brand"),
        F.col("product_summary.latest_last_restock"),
        F.col("product_summary.quantity_count"),
    )
    .orderBy("quantity_count", "id")
    .limit(20)
)
