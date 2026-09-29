from pyspark.sql import functions as F

valid_store_sales = df_store_sales.filter((F.col("units") == 16) & (F.col("revenue") > 3100))

store_sales_stats = (
    valid_store_sales
    .groupBy("product_id")
    .agg(F.sum("revenue").alias("total_revenue"))
)

result = (
    store_sales_stats
    .filter(F.col("total_revenue") < 23000)
    .select("product_id", "total_revenue")
    .orderBy(F.col("total_revenue").desc())
)
