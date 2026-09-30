from pyspark.sql import functions as F

product_stats = (
    df_order_items
    .groupBy("product_id")
    .agg(F.count("quantity").alias("quantity_count"))
)

result = (
    df_products.alias("p")
    .join(product_stats.alias("ps"), F.col("p.id") == F.col("ps.product_id"), "left")
    .filter(F.col("ps.quantity_count") >= 8)
    .select(F.col("p.stock_qty"), F.col("p.category"), F.col("ps.quantity_count"))
    .orderBy("category", F.col("stock_qty").desc())
)
