from pyspark.sql import functions as F

src = (
    df_store_sales
    .filter((F.year("sale_date") >= 2023) & F.col("product_id").isin([5, 8]))
)

store_sales_stats = (
    src
    .groupBy(
        F.when(F.col("revenue") >= 4000, "high").when(F.col("revenue") >= 800, "mid").otherwise("low").alias("revenue_bucket"),
    )
    .agg(
        F.sum("units").alias("total_units"),
        F.avg("revenue").alias("avg_revenue"),
    )
)

result = (
    store_sales_stats
    .filter(F.col("avg_revenue") >= 1700)
    .select("revenue_bucket", "avg_revenue")
    .orderBy("revenue_bucket")
)
