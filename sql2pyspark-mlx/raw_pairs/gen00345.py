from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("product_id").isin([5, 6, 10]))
    .select("id", "store_id", "product_id")
)
