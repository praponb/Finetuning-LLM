from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy(F.month("order_date").alias("order_month"))
    .agg(
        F.round(F.stddev("discount"), 2).alias("stddev_discount"),
        F.avg("shipping_fee").alias("avg_shipping_fee"),
    )
    .orderBy(F.col("stddev_discount").desc())
)
