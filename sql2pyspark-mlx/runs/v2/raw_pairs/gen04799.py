from pyspark.sql import functions as F

result = (
    df_store_sales
    .agg(
        F.round(F.stddev("units"), 2).alias("stddev_units"),
        F.avg("revenue").alias("avg_revenue"),
    )
    .orderBy("avg_revenue")
)
