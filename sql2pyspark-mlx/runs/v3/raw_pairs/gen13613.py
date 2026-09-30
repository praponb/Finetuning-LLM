from pyspark.sql import functions as F

result = (
    df_store_sales
    .groupBy(
        F.when(F.col("revenue") >= 4200, "high").when(F.col("revenue") >= 1000, "mid").otherwise("low").alias("revenue_bucket"),
    )
    .agg(
        F.min("sale_date").alias("min_sale_date"),
        F.sum("revenue").alias("total_revenue"),
    )
    .orderBy(F.col("min_sale_date").desc(), "revenue_bucket")
    .limit(20)
)
