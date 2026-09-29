from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy(F.quarter("order_date").alias("order_quarter"))
    .agg(
        F.count("*").alias("order_count"),
        F.round(F.stddev("discount"), 2).alias("stddev_discount"),
    )
    .filter(F.col("stddev_discount") >= 63)
    .orderBy("order_count")
)
