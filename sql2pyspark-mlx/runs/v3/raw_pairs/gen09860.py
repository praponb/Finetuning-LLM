from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("store_id").isin([1, 2, 5, 6]))
    .select("id", "store_id", "quantity", "product_id")
)
