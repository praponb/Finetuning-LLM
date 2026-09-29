from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(((F.col("product_id") == 10) & F.col("store_id").isin([3, 5])) | F.month("sale_date").isin([5, 6, 8]))
    .groupBy("product_id")
    .agg(
        F.count("revenue").alias("revenue_count"),
        F.avg("revenue").alias("avg_revenue"),
        F.min("units").alias("min_units"),
    )
)
