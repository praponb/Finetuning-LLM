from pyspark.sql import functions as F

result = (
    df_store_sales
    .agg(
        F.avg("units").alias("avg_units"),
        F.round(F.stddev("revenue"), 2).alias("stddev_revenue"),
        F.sum("units").alias("total_units"),
    )
    .orderBy(F.col("avg_units").desc())
)
