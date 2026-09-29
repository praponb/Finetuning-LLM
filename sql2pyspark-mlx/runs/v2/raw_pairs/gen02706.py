from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter((F.col("product_id") == 7) & F.col("product_id").isin([3, 4, 5, 7]) & F.col("revenue").between(3600, 3900))
    .groupBy(F.date_trunc("MONTH", "sale_date").alias("sale_month"))
    .agg(F.sum("revenue").alias("total_revenue"))
    .filter(F.col("total_revenue") >= 20000)
)
