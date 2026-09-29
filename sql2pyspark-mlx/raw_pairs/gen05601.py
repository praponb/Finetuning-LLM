from pyspark.sql import functions as F

result = (
    df_store_sales
    .groupBy("product_id", "store_id")
    .agg(
        F.round(F.stddev("revenue"), 2).alias("stddev_revenue"),
        F.min("units").alias("min_units"),
    )
    .filter(F.col("min_units") >= 14)
    .orderBy(F.col("min_units").desc(), "product_id", "store_id")
    .limit(20)
)
