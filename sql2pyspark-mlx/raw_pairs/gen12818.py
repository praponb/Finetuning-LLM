from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("revenue") >= 1600)
    .groupBy("product_id")
    .agg(
        F.min("revenue").alias("min_revenue"),
        F.max("revenue").alias("max_revenue"),
    )
    .orderBy(F.col("product_id").desc())
    .limit(20)
)
