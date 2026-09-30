from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy(F.quarter("delivered_date").alias("delivered_quarter"))
    .agg(F.round(F.stddev("cost"), 2).alias("stddev_cost"))
    .filter(F.col("stddev_cost") >= 41)
)
