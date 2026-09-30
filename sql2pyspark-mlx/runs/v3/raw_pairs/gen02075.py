from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("units") <= 16)
    .groupBy("product_id")
    .agg(F.round(F.stddev("units"), 2).alias("stddev_units"))
    .filter(F.col("stddev_units") < 59)
    .select("product_id", "stddev_units")
)
