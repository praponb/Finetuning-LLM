from pyspark.sql import functions as F

result = (
    df_inventory
    .filter((F.col("store_id").isin([4, 6]) & (F.col("quantity") <= 212)) | F.col("last_restock").between("2023-11-06", "2025-08-08"))
    .orderBy("last_restock")
)
