from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("store_id").isin([3, 5, 6]))
    .select("product_id", "last_restock")
)
