from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("store_id").isin([2, 4]))
    .select("id", "product_id", "store_id", "last_restock")
)
