from pyspark.sql import functions as F

recent_store_sales = (
    df_store_sales
    .filter(((F.col("revenue") >= 2300) & F.col("product_id").isin([1, 4])) | (F.col("units") < 24))
)

store_sales_stats = (
    recent_store_sales
    .groupBy("product_id")
    .agg(F.sum("revenue").alias("total_revenue"))
)

result = (
    store_sales_stats
    .filter(F.col("total_revenue") < 19000)
    .select("product_id", "total_revenue")
)
