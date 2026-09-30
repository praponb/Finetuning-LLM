from pyspark.sql import functions as F

result = (
    df_store_sales
    .groupBy("product_id")
    .agg(
        F.count("revenue").alias("revenue_count"),
        F.countDistinct("revenue").alias("unique_products"),
    )
)
