from pyspark.sql import functions as F

result = (
    df_store_sales
    .groupBy("product_id")
    .agg(
        F.countDistinct("revenue").alias("unique_products"),
        F.avg("revenue").alias("avg_revenue"),
        F.max("revenue").alias("max_revenue"),
    )
)
