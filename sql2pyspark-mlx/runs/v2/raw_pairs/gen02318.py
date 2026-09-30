from pyspark.sql import functions as F

base = df_store_sales.filter(F.col("product_id").isin([5, 10]))

summary = (
    base
    .groupBy(F.date_format("sale_date", "yyyy-MM").alias("sale_month"), "product_id")
    .agg(F.avg("revenue").alias("avg_revenue"))
)

result = (
    summary
    .filter(F.col("avg_revenue") < 2500)
    .select("sale_month", "product_id", "avg_revenue")
    .orderBy("sale_month", "product_id")
    .limit(20)
)
