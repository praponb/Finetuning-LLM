from pyspark.sql import functions as F

recent_store_sales = (
    df_store_sales
    .filter(F.col("sale_date") < "2024-02-06")
    .withColumn("adjusted_units", F.round((F.col("units") * 0.85), 2))
)

store_sales_stats = (
    recent_store_sales
    .groupBy("product_id")
    .agg(F.min("units").alias("min_units"))
)

result = store_sales_stats
