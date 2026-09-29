from pyspark.sql import functions as F

result = (
    df_inventory
    .filter((F.col("store_id").isin([1, 3, 4, 5]) & F.col("last_restock").between("2023-08-01", "2024-04-14")) | F.col("store_id").isin([1, 2, 3, 4]))
    .agg(F.round(F.avg("quantity"), 2).alias("avg_quantity"))
    .orderBy(F.col("avg_quantity").desc())
)
