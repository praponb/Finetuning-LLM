from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(~(F.col("store_id").isin([1, 2, 4]) | F.col("store_id").isin([3, 4, 5, 6])))
    .select("last_restock", "store_id", "quantity")
)
