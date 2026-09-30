from pyspark.sql import functions as F

product_summary = (
    df_inventory
    .filter(F.col("last_restock") > "2023-11-11")
    .groupBy("product_id")
    .agg(
        F.sum("quantity").alias("total_quantity"),
        (F.max("quantity") - F.min("quantity")).alias("quantity_range"),
    )
)

result = (
    df_products.alias("products")
    .join(product_summary.alias("product_summary"), F.col("products.id") == F.col("product_summary.product_id"), "inner")
    .select(
        F.col("products.brand"),
        F.col("product_summary.total_quantity"),
        F.col("product_summary.quantity_range"),
    )
)
