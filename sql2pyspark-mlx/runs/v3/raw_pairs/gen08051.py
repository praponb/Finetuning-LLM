from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(((F.col("product_id") == 1) & (F.col("store_id") == 2)) | F.col("store_id").isin([2, 5]))
    .withColumn("days_since_last_restock", F.datediff(F.current_date(), "last_restock"))
)
