from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("product_id").isin([1, 2, 4, 9]))
    .groupBy("store_id", F.date_format("sale_date", "yyyy-MM").alias("sale_month"))
    .agg(F.max("revenue").alias("max_revenue"))
    .filter(F.col("max_revenue") > 1200)
    .select("store_id", "sale_month", "max_revenue")
)
