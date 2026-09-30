from pyspark.sql import functions as F

recent_store_sales = (
    df_store_sales
    .filter(F.col("product_id").isin([6, 10]) & F.col("store_id").isin([5, 6]))
)

agg = (
    recent_store_sales
    .groupBy(F.date_format("sale_date", "yyyy-MM").alias("sale_month"))
    .agg(F.min("sale_date").alias("first_sale_date"))
)

result = agg.select("sale_month", "first_sale_date")
