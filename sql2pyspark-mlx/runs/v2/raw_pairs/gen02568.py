from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("store_id").isin([1, 4, 5, 6]))
    .groupBy("store_id", "product_id")
    .agg(F.count("units").alias("units_count"))
    .filter(F.col("units_count") >= 5)
)
