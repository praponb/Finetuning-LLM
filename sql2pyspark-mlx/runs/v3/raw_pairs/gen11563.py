from pyspark.sql import functions as F

recent_store_sales = df_store_sales.filter(F.col("units") != 38)

store_sales_stats = (
    recent_store_sales
    .groupBy(F.date_format("sale_date", "yyyy-MM").alias("sale_month"))
    .agg(F.round(F.avg("revenue"), 2).alias("avg_revenue"))
)

result = (
    store_sales_stats
    .filter(F.col("avg_revenue") < 62)
    .select("sale_month", "avg_revenue")
    .orderBy("avg_revenue", "sale_month")
    .limit(3)
)
