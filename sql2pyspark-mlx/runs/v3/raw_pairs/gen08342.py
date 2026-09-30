from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("units") > 34)
    .groupBy("product_id", "store_id")
    .agg(
        F.avg("revenue").alias("avg_revenue"),
        F.min("revenue").alias("min_revenue"),
    )
    .filter(F.col("min_revenue") < 1900)
    .orderBy("product_id")
)
