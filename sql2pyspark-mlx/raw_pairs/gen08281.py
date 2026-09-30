from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("quantity").between(195, 208) & (F.col("quantity").isNull() | F.col("store_id").isin([3, 4, 6])))
    .select("last_restock")
)
