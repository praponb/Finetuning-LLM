from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("store_id").isin([3, 5]))
    .select("id", "quantity", "store_id", "last_restock")
)
