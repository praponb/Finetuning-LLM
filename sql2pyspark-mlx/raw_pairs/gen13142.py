from pyspark.sql import functions as F

result = (
    df_store_sales
    .groupBy("product_id")
    .agg(
        F.round(F.stddev("revenue"), 2).alias("stddev_revenue"),
        F.sum("units").alias("total_units"),
    )
    .orderBy(F.col("total_units").desc())
)
