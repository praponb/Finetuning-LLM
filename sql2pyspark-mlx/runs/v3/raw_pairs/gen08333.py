from pyspark.sql import functions as F

valid_store_sales = df_store_sales.filter(F.col("revenue") <= 2000)

store_sales_stats = (
    valid_store_sales
    .groupBy(F.date_format("sale_date", "yyyy-MM").alias("sale_month"))
    .agg(F.count("*").alias("cnt"))
)

result = (
    store_sales_stats
    .select("sale_month", "cnt")
    .orderBy("cnt")
)
