from pyspark.sql import functions as F

result = (
    df_store_sales
    .groupBy("store_id")
    .agg(
        F.round(F.stddev("units"), 2).alias("stddev_units"),
        F.min("revenue").alias("min_revenue"),
    )
    .filter(F.col("min_revenue") > 2200)
    .select("store_id", "stddev_units", "min_revenue")
    .orderBy(F.col("store_id").desc())
)
