from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("store_id").isin([2, 5, 6]) & F.col("store_id").isin([3, 5, 6]))
    .withColumn("last_restock_plus_months", F.add_months("last_restock", 3))
    .withColumn("last_restock_month_end", F.last_day("last_restock"))
)
