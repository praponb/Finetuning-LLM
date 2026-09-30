from pyspark.sql import functions as F

result = (
    df_store_sales
    .groupBy("product_id")
    .agg(
        F.max("revenue").alias("max_revenue"),
        F.min("revenue").alias("min_revenue"),
    )
    .filter(F.col("max_revenue") >= 3900)
    .orderBy(F.col("product_id").desc())
    .limit(50)
)
