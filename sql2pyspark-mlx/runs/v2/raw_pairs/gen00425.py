from pyspark.sql import functions as F

filtered_store_sales = df_store_sales.filter(F.col("product_id").isin([5, 6]) & F.col("units").isNull())

store_sales_stats = (
    filtered_store_sales
    .groupBy("store_id")
    .agg(F.count("units").alias("units_count"))
)

result = store_sales_stats.filter(F.col("units_count") > 8)
