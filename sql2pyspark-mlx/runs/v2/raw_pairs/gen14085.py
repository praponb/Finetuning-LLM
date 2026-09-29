from pyspark.sql import functions as F

product_summary = (
    df_order_items
    .groupBy("product_id")
    .agg(F.countDistinct("quantity").alias("unique_products"))
)

result = (
    df_products.alias("p")
    .join(product_summary.alias("ps"), F.col("p.id") == F.col("ps.product_id"), "inner")
    .filter(F.col("ps.unique_products") > 6)
    .select(
        F.col("p.id"),
        F.col("p.price"),
        F.col("p.supplier_id"),
        F.col("ps.unique_products"),
    )
    .orderBy("supplier_id", "id")
    .limit(50)
)
