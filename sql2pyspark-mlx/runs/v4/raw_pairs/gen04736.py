from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("product_id").isin([2, 4, 7, 10]))
    .agg(
        (F.max("revenue") - F.min("revenue")).alias("revenue_range"),
        F.round(F.stddev("revenue"), 2).alias("stddev_revenue"),
    )
    .orderBy(F.col("stddev_revenue").desc())
)
