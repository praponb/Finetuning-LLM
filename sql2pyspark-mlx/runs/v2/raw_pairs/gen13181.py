from pyspark.sql import functions as F

result = (
    df_store_sales
    .groupBy("product_id")
    .agg(
        F.avg("revenue").alias("avg_revenue"),
        F.min("revenue").alias("min_revenue"),
    )
    .orderBy("avg_revenue")
)
