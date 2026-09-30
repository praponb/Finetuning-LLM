from pyspark.sql import functions as F

result = (
    df_inventory
    .filter((F.col("store_id") == 3) & F.col("store_id").isin([2, 6]))
    .withColumn("last_restock_period", F.date_trunc("WEEK", "last_restock"))
)
