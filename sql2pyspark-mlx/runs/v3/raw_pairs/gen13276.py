from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("product_id").isin([1, 5]))
    .groupBy("store_id")
    .agg(F.count("quantity").alias("quantity_count"))
    .filter(F.col("quantity_count") >= 8)
    .select("store_id", "quantity_count")
)
