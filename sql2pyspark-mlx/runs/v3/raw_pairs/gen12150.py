from pyspark.sql import functions as F

product_stats = (
    df_order_items
    .groupBy("product_id")
    .agg(
        F.min("quantity").alias("min_quantity"),
        F.count("*").alias("total_count"),
    )
)

result = (
    df_products.alias("p")
    .join(product_stats.alias("ps"), F.col("p.id") == F.col("ps.product_id"), "inner")
    .select(
        F.col("p.id"),
        F.col("p.supplier_id"),
        F.col("ps.min_quantity"),
        F.col("ps.total_count"),
    )
)
