from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("store_id").isin([4, 5]))
    .groupBy("store_id", F.month("last_restock").alias("last_restock_month"))
    .agg(F.max("quantity").alias("max_quantity"))
)
