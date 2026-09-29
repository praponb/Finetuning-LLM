from pyspark.sql import functions as F

product_summary = (
    df_order_items
    .filter(F.col("product_id").isNull())
    .groupBy("product_id")
    .agg(F.count("quantity").alias("quantity_count"))
)

result = (
    df_products.alias("p")
    .join(product_summary.alias("ps"), F.col("p.id") == F.col("ps.product_id"), "left")
    .select(F.col("p.brand"), F.col("p.category"), F.col("ps.quantity_count"))
    .orderBy("brand")
)
