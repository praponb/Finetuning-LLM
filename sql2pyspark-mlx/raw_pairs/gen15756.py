from pyspark.sql import functions as F

result = (
    df_store_sales
    .groupBy("store_id")
    .agg(
        F.round(F.stddev("revenue"), 2).alias("stddev_revenue"),
        F.min("units").alias("min_units"),
        F.sum("revenue").alias("total_revenue"),
    )
    .filter(F.col("total_revenue") < 14000)
    .drop("total_revenue")
    .orderBy(F.col("min_units").desc(), "store_id")
    .limit(10)
)
