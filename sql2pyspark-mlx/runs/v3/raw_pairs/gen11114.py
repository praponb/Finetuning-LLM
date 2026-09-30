from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter((F.col("units") >= 35) & F.col("store_id").isin([5, 6]) & (F.col("revenue") >= 1500))
    .groupBy("store_id")
    .agg(
        F.round(F.avg("units"), 2).alias("avg_units"),
        F.round(F.stddev("units"), 2).alias("stddev_units"),
        F.max("units").alias("max_units"),
    )
    .filter(F.col("max_units") > 29)
    .drop("max_units")
)
