from pyspark.sql import functions as F

product_summary = (
    df_order_items
    .groupBy("product_id")
    .agg(
        F.countDistinct("quantity").alias("unique_orders"),
        F.count("quantity").alias("quantity_count"),
    )
)

result = (
    df_products.alias("p")
    .join(product_summary.alias("ps"), F.col("p.id") == F.col("ps.product_id"), "inner")
    .filter(F.col("ps.unique_orders") >= 5)
    .select(
        F.col("p.id"),
        F.col("p.supplier_id"),
        F.col("ps.unique_orders"),
        F.col("ps.quantity_count"),
    )
    .orderBy(F.col("id").desc())
)
