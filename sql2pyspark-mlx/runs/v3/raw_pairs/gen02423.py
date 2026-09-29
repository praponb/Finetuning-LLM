from pyspark.sql import functions as F

src = df_store_sales.filter(F.month("sale_date").isin([3, 9, 10]))

summary = (
    src
    .groupBy(
        F.when(F.col("revenue") >= 3000, "high").when(F.col("revenue") >= 2000, "mid").otherwise("low").alias("revenue_bucket"),
        F.date_trunc("MONTH", "sale_date").alias("sale_month"),
    )
    .agg(F.round(F.avg("revenue"), 2).alias("avg_revenue"))
)

result = (
    summary
    .filter(F.col("avg_revenue") >= 28)
    .orderBy("revenue_bucket", "sale_month")
    .limit(5)
)
