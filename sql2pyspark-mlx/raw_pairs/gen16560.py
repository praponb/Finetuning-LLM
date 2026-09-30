from pyspark.sql import functions as F

product_summary = (
    df_order_items
    .filter(F.col("product_id") == 35)
    .groupBy("product_id")
    .agg(F.min("quantity").alias("min_quantity"))
)

result = (
    df_products.alias("p")
    .join(product_summary.alias("ps"), F.col("p.id") == F.col("ps.product_id"), "left")
    .filter(F.col("ps.min_quantity") >= 8.1)
    .select(F.col("p.category"), F.col("p.price"), F.col("ps.min_quantity"))
    .orderBy("category", "min_quantity")
)
