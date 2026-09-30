from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("store_id") == 2)
    .groupBy("product_id", "store_id")
    .agg(F.count("*").alias("total_count"))
    .filter(F.col("total_count") >= 3)
    .orderBy(F.col("product_id").desc(), "store_id")
    .limit(3)
)
