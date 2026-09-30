from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("product_id").isin([4, 6, 10]))
    .groupBy("store_id", F.date_trunc("MONTH", "sale_date").alias("sale_month"))
    .agg(F.avg("revenue").alias("avg_revenue"))
    .orderBy(F.col("avg_revenue").desc(), "store_id", "sale_month")
    .limit(50)
)
