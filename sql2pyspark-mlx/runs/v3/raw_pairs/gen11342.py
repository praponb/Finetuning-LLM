from pyspark.sql import functions as F

product_summary = (
    df_inventory
    .groupBy("product_id")
    .agg(
        F.avg("quantity").alias("avg_quantity"),
        F.min("last_restock").alias("min_last_restock"),
    )
)

result = (
    df_products.alias("products")
    .join(product_summary.alias("t"), F.col("products.id") == F.col("t.product_id"), "inner")
    .select(
        F.col("products.id"),
        F.col("products.supplier_id"),
        F.col("products.product_name"),
        F.col("t.avg_quantity"),
        F.col("t.min_last_restock"),
    )
    .orderBy("avg_quantity")
)
