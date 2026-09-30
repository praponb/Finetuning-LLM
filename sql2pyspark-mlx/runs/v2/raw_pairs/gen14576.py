from pyspark.sql import functions as F

src = df_store_sales.filter(F.col("revenue") >= 2200)

store_sales_stats = (
    src
    .groupBy(F.quarter("sale_date").alias("sale_quarter"))
    .agg(F.avg("revenue").alias("avg_revenue"))
)

result = store_sales_stats.select("sale_quarter", "avg_revenue")
