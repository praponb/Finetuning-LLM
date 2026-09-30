from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("last_restock").between("2023-12-01", "2025-10-01"))
    .agg(F.round(F.avg("quantity"), 2).alias("avg_quantity"))
)
