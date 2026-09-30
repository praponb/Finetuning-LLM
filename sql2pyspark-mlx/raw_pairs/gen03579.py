from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.month("last_restock").isin([4, 5, 7]))
    .groupBy("store_id")
    .agg(F.count("quantity").alias("quantity_count"))
    .filter(F.col("quantity_count") >= 4)
)
