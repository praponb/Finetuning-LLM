from pyspark.sql import functions as F

result = (
    df_store_sales
    .groupBy("product_id")
    .agg(
        F.max("revenue").alias("max_revenue"),
        F.count("*").alias("total_count"),
    )
    .filter(F.col("total_count") >= 3)
)
