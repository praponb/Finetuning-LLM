from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("quantity") < 62)
    .groupBy("store_id", F.quarter("last_restock").alias("last_restock_quarter"))
    .agg(F.countDistinct("quantity").alias("unique_stores"))
)
