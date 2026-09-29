from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("product_id").isin([2, 4, 6, 7]))
    .groupBy(
        F.when(F.col("revenue") >= 3600, "high").when(F.col("revenue") >= 1700, "mid").otherwise("low").alias("revenue_bucket"),
    )
    .agg(
        F.avg("revenue").alias("avg_revenue"),
        F.min("sale_date").alias("first_sale_date"),
        F.max("revenue").alias("max_revenue"),
    )
)
