from pyspark.sql import functions as F

result = (
    df_inventory
    .groupBy("store_id", "product_id")
    .agg(F.count("*").alias("total_count"))
    .filter(F.col("total_count") < 6)
    .select("store_id", "product_id", "total_count")
)
