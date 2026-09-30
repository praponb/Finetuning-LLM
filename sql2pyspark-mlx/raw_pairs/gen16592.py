from pyspark.sql import functions as F

product_stats = (
    df_inventory
    .filter(F.col("last_restock").between("2023-03-06", "2025-03-01"))
    .groupBy("product_id")
    .agg((F.max("quantity") - F.min("quantity")).alias("quantity_range"))
)

result = (
    df_products.alias("p")
    .join(product_stats.alias("ps"), F.col("p.id") == F.col("ps.product_id"), "inner")
    .filter(F.col("ps.quantity_range") >= 72)
    .select(F.col("p.id"), F.col("p.launch_date"), F.col("ps.quantity_range"))
)
