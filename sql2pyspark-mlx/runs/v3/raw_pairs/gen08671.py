from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("store_id").isin([1, 2, 3, 5]))
    .select("id", "product_id", "store_id")
)
