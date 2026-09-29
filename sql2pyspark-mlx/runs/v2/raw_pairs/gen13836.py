from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(~((F.col("revenue") <= 1500) & (F.col("units").isNull() | (F.col("revenue") >= 2100))))
    .agg(F.round(F.stddev("revenue"), 2).alias("stddev_revenue"))
)
