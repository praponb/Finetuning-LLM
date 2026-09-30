from pyspark.sql import functions as F

product_stats = (
    df_inventory
    .groupBy("product_id")
    .agg(F.avg("quantity").alias("avg_quantity"))
)

result = (
    df_products.alias("p")
    .join(product_stats.alias("ps"), F.col("p.id") == F.col("ps.product_id"), "left")
    .filter(F.col("ps.avg_quantity") >= 200)
    .select(F.col("p.supplier_id"), F.col("p.stock_qty"), F.col("ps.avg_quantity"))
    .orderBy("supplier_id", "avg_quantity")
)
