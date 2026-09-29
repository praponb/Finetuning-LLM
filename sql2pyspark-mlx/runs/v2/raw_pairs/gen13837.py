from pyspark.sql import functions as F

product_totals = (
    df_inventory
    .filter(F.col("product_id") == 8)
    .groupBy("product_id")
    .agg(
        F.count("*").alias("total_count"),
        F.min("last_restock").alias("first_last_restock"),
    )
)

result = (
    df_products.alias("products")
    .join(product_totals.alias("product_totals"), F.col("products.id") == F.col("product_totals.product_id"), "left")
    .select(
        F.col("products.supplier_id"),
        F.col("product_totals.total_count"),
        F.col("product_totals.first_last_restock"),
    )
)
