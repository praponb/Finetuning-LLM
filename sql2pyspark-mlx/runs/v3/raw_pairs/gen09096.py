from pyspark.sql import functions as F

result = (
    df_store_sales
    .agg(
        F.count("revenue").alias("revenue_count"),
        F.round(F.stddev("revenue"), 2).alias("stddev_revenue"),
    )
    .orderBy(F.col("stddev_revenue").desc(), "revenue_count")
)
