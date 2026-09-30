from pyspark.sql import functions as F

product_summary = (
    df_inventory
    .groupBy("product_id")
    .agg(F.avg("quantity").alias("avg_quantity"))
)

result = (
    df_products.alias("p")
    .join(product_summary.alias("ps"), F.col("p.id") == F.col("ps.product_id"), "left")
    .filter(F.col("ps.avg_quantity") >= 110)
    .select(F.col("p.supplier_id"), F.col("p.stock_qty"), F.col("ps.avg_quantity"))
    .orderBy("avg_quantity")
)
