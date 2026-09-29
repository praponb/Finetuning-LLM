from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("store_id").isin([2, 3, 4, 5]))
    .agg(F.count("quantity").alias("quantity_count"))
)
