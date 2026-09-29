from pyspark.sql import functions as F

result = (
    df_inventory
    .filter((F.col("last_restock") >= "2024-02-18") & (F.col("store_id").isin([2, 4, 5, 6]) | F.col("store_id").isin([3, 4])))
    .select("id", "quantity", "store_id", "last_restock")
)
