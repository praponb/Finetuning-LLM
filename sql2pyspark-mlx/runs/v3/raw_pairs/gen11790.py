from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy("status")
    .agg(
        F.sum("discount").alias("total_discount"),
        F.round(F.stddev("shipping_fee"), 2).alias("stddev_shipping_fee"),
    )
    .filter(F.col("total_discount") >= 1.2)
    .select("status", "total_discount", "stddev_shipping_fee")
    .orderBy("stddev_shipping_fee")
)
