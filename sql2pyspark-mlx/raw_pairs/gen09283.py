from pyspark.sql import functions as F

result = (
    df_inventory
    .groupBy("store_id", "product_id")
    .agg(F.count("*").alias("num_inventory"))
    .filter(F.col("num_inventory") >= 10)
    .orderBy(F.col("num_inventory").desc())
)
