from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("store_id").isin([3, 6]) & (F.col("product_id") == 4))
    .select("id", "product_id", "store_id", "quantity")
)
