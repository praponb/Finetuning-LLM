from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter((F.col("product_id") == 9) & (F.col("revenue") < 3100))
    .groupBy("product_id")
    .agg(F.count("units").alias("units_count"))
    .filter(F.col("units_count") < 4)
)
