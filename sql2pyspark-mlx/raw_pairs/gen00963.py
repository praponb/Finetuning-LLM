from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.month("last_restock").isin([6, 9, 10]) | (F.col("last_restock") >= "2025-01-01"))
    .select("id", "last_restock", "store_id")
    .orderBy(F.col("last_restock").desc())
)
