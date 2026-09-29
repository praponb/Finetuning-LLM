from pyspark.sql import functions as F

result = (
    df_inventory
    .groupBy("store_id")
    .agg(F.count("quantity").alias("quantity_count"))
    .orderBy(F.col("quantity_count").desc(), F.col("store_id").desc())
    .limit(10)
)
