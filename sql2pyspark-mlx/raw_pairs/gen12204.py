from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("revenue") >= 2500)
    .groupBy("store_id")
    .agg(
        F.round(F.stddev("units"), 2).alias("stddev_units"),
        F.avg("units").alias("avg_units"),
    )
)
