from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("channel") == "store")
    .groupBy("channel")
    .agg(
        F.countDistinct("shipping_fee").alias("unique_statuss"),
        F.round(F.stddev("discount"), 2).alias("stddev_discount"),
        F.round(F.stddev("shipping_fee"), 2).alias("stddev_shipping_fee"),
    )
    .orderBy(F.col("stddev_discount").desc(), F.col("stddev_shipping_fee").desc())
)
