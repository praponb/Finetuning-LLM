from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.filter(F.col("launch_date").between("2023-03-01", "2024-01-01")).alias("p"), F.col("i.product_id") == F.col("p.id"), "left_anti")
    .filter(F.col("i.product_id").isin([3, 7, 8, 9]))
    .select(F.col("i.id"), F.col("i.quantity"), F.col("i.last_restock"))
)
