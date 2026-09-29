from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy("channel", "status")
    .agg(
        F.round(F.stddev("shipping_fee"), 2).alias("stddev_shipping_fee"),
        F.min("shipping_fee").alias("min_shipping_fee"),
    )
    .orderBy(F.col("status").desc())
)
