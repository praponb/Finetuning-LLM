from pyspark.sql import functions as F

product_stats = (
    df_inventory
    .filter(F.col("store_id").isin([4, 6]))
    .groupBy("product_id")
    .agg(F.avg("quantity").alias("avg_quantity"))
)

result = (
    df_products.alias("p")
    .join(product_stats.alias("ps"), F.col("p.id") == F.col("ps.product_id"), "left")
    .filter(F.col("ps.avg_quantity") >= 110)
    .select(F.col("p.id"), F.col("p.supplier_id"), F.col("ps.avg_quantity"))
)
