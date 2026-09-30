from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("revenue") <= 2700)
    .groupBy("product_id")
    .agg(F.round(F.stddev("units"), 2).alias("stddev_units"))
    .filter(F.col("stddev_units") >= 34)
    .orderBy(F.col("stddev_units").desc(), F.col("product_id").desc())
    .limit(1)
)
