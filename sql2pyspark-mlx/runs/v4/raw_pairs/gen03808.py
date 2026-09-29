from pyspark.sql import functions as F

result = (
    df_store_sales
    .groupBy("store_id")
    .agg(
        F.round(F.stddev("units"), 2).alias("stddev_units"),
        F.countDistinct("units").alias("unique_products"),
    )
    .filter(F.col("stddev_units") >= 67)
    .select("store_id", "stddev_units", "unique_products")
)
