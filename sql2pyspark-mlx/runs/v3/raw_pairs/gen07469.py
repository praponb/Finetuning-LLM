from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("quantity") < 150)
    .groupBy("store_id")
    .agg(F.count("quantity").alias("quantity_count"))
    .filter(F.col("quantity_count") >= 10)
    .select("store_id", "quantity_count")
    .orderBy(F.col("quantity_count").desc(), "store_id")
    .limit(5)
)
