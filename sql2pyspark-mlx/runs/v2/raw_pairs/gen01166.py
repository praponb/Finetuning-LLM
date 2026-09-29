from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("product_id").isin([4, 9, 10]))
    .withColumn("days_since_last_restock", F.datediff(F.current_date(), "last_restock"))
)
