from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("product_id").isin([2, 3]) & (F.col("last_restock") < "2023-06-06") & F.col("product_id").isin([9, 10]))
)
