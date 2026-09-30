from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("store_id").isin([3, 4]) | F.month("last_restock").isin([4, 7, 9]))
    .withColumn("days_since_last_restock", F.datediff(F.current_date(), "last_restock"))
)
