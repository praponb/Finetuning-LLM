from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy("status")
    .agg(F.round(F.stddev("discount"), 2).alias("stddev_discount"))
    .filter(F.col("stddev_discount") > 79)
    .select("status", "stddev_discount")
    .orderBy("status")
)
