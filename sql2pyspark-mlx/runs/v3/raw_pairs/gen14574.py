from pyspark.sql import functions as F

product_stats = (
    df_order_items
    .filter(F.col("product_id").isin([11, 13, 20, 27]))
    .groupBy("product_id")
    .agg(
        F.min("unit_price").alias("min_unit_price"),
        F.count("quantity").alias("quantity_count"),
    )
)

result = (
    df_products.alias("p")
    .join(product_stats.alias("ps"), F.col("p.id") == F.col("ps.product_id"), "left")
    .select(
        F.col("p.id"),
        F.col("p.supplier_id"),
        F.col("ps.min_unit_price"),
        F.col("ps.quantity_count"),
    )
    .orderBy(F.col("id").desc())
)
