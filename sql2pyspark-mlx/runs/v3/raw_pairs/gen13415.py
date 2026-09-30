from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("store_id").isin([1, 3, 4, 6]))
    .select("id", "last_restock", "quantity", "store_id")
    .orderBy(F.col("store_id").desc())
)
