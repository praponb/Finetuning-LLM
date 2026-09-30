from pyspark.sql import functions as F

result = (
    df_store_sales
    .groupBy(F.quarter("sale_date").alias("sale_quarter"), "store_id")
    .agg(
        F.round(F.avg("revenue"), 2).alias("avg_revenue"),
        F.round(F.stddev("revenue"), 2).alias("stddev_revenue"),
    )
    .filter(F.col("stddev_revenue") > 73)
    .orderBy(F.col("stddev_revenue").desc())
)
