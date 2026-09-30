from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("product_id").isin([6, 10]) & (F.col("last_restock") > "2023-07-01") & F.col("store_id").isin([1, 3, 5, 6]))
    .agg(F.count("quantity").alias("quantity_count"))
)
