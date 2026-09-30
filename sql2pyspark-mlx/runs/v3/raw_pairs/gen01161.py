from pyspark.sql import functions as F

result = (
    df_store_sales
    .groupBy("product_id")
    .agg(
        F.min("revenue").alias("min_revenue"),
        F.round(F.stddev("revenue"), 2).alias("stddev_revenue"),
    )
    .orderBy(F.col("stddev_revenue").desc())
)
