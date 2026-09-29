from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("units") == 21)
    .groupBy(
        F.when(F.col("revenue") >= 3800, "high").when(F.col("revenue") >= 1700, "mid").otherwise("low").alias("revenue_bucket"),
        "product_id",
        F.month("sale_date").alias("sale_month"),
    )
    .agg(
        F.min("sale_date").alias("min_sale_date"),
        F.avg("revenue").alias("avg_revenue"),
    )
    .orderBy("min_sale_date", "revenue_bucket")
)
