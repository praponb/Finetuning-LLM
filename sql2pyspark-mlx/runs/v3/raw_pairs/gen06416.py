from pyspark.sql import functions as F

result = (
    df_store_sales
    .groupBy(F.quarter("sale_date").alias("sale_quarter"))
    .agg(
        F.round(F.stddev("revenue"), 2).alias("stddev_revenue"),
        F.round(F.avg("revenue"), 2).alias("avg_revenue"),
    )
)
