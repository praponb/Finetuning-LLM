from pyspark.sql import functions as F

result = (
    df_inventory
    .filter((F.col("last_restock") > "2023-05-01") & ((F.col("store_id") == 6) | F.col("store_id").isin([1, 6])))
    .withColumn("last_restock_month_end", F.last_day("last_restock"))
    .orderBy("last_restock_month_end", "last_restock")
)
