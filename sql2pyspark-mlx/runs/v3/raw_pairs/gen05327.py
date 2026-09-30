from pyspark.sql import functions as F

result = (
    df_inventory
    .groupBy("store_id")
    .agg(F.count("quantity").alias("quantity_count"))
    .filter(F.col("quantity_count") < 2)
)
