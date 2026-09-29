from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("product_id").isin([3, 5, 9, 10]) & (F.col("revenue") > 2500))
    .groupBy("product_id", "store_id")
    .agg(F.count("units").alias("units_count"))
)
