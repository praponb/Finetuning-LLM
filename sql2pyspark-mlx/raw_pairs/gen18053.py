from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(((F.col("product_id") == 6) & (F.col("store_id") == 5)) | (F.col("revenue") >= 3500))
    .agg(
        F.round(F.stddev("units"), 2).alias("stddev_units"),
        F.max("units").alias("max_units"),
        F.round(F.stddev("revenue"), 2).alias("stddev_revenue"),
    )
)
