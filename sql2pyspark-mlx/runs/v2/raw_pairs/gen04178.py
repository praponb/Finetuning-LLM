from pyspark.sql import functions as F

base = df_store_sales.filter(F.col("store_id").isin([2, 5]))

store_sales_stats = (
    base
    .groupBy(F.date_trunc("MONTH", "sale_date").alias("sale_month"), "product_id")
    .agg(F.count("*").alias("store_sale_count"))
)

result = (
    store_sales_stats
    .orderBy(F.col("product_id").desc(), "sale_month")
    .limit(50)
)
