from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("store_id").isin([2, 5]))
    .select("id", "product_id", "last_restock")
)
