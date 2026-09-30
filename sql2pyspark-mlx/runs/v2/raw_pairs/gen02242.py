from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("product_id").isin([5, 6, 9]) & (F.year("sale_date") == 2024))
    .groupBy(F.date_format("sale_date", "yyyy-MM").alias("sale_month"), "store_id")
    .agg(
        (F.max("revenue") - F.min("revenue")).alias("revenue_range"),
        F.count("revenue").alias("revenue_count"),
        F.avg("revenue").alias("avg_revenue"),
    )
    .filter(F.col("avg_revenue") >= 1700)
    .drop("avg_revenue")
    .orderBy(F.col("store_id").desc())
)
