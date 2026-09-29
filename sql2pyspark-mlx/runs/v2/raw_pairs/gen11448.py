from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("store_id").isin([2, 3]) & F.col("product_id").isin([3, 6, 8, 10]) & (F.col("product_id") == 10))
)
