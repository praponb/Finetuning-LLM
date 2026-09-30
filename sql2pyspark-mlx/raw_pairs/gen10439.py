from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("last_restock").between("2024-12-01", "2025-02-01"))
    .select("last_restock")
    .orderBy("last_restock")
)
