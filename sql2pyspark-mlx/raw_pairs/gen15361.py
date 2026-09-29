from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("store_id").isin([1, 2, 5]) & (F.col("last_restock") <= "2023-11-01"))
    .select("id", "quantity", "store_id", "last_restock")
)
