from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("last_restock").between("2023-07-01", "2024-01-27"))
    .groupBy("product_id")
    .agg((F.max("quantity") - F.min("quantity")).alias("quantity_range"))
    .orderBy("quantity_range", F.col("product_id").desc())
    .limit(10)
)
