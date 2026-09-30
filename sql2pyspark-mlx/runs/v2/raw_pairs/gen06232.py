from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("store_id").isin([1, 2, 5]))
    .groupBy("store_id")
    .agg(
        F.round(F.stddev("revenue"), 2).alias("stddev_revenue"),
        F.count("units").alias("units_count"),
        F.min("sale_date").alias("min_sale_date"),
    )
    .filter(F.col("stddev_revenue") > 54)
    .orderBy(F.col("stddev_revenue").desc())
)
