from pyspark.sql import functions as F

recent_inventory = (
    df_inventory
    .filter(F.col("quantity") < 169)
    .withColumn("quantity_whole", F.floor("quantity"))
)

summary = (
    recent_inventory
    .groupBy("store_id")
    .agg(F.count("quantity").alias("quantity_count"))
)

result = summary.filter(F.col("quantity_count") < 9)
