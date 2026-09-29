from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("store_id").isin([1, 2, 3, 4]))
    .groupBy("store_id")
    .agg(F.round(F.stddev("units"), 2).alias("stddev_units"))
)
