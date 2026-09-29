from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy("channel", "status")
    .agg(F.round(F.stddev("discount"), 2).alias("stddev_discount"))
    .filter(F.col("stddev_discount") < 39)
    .select("channel", "status", "stddev_discount")
)
