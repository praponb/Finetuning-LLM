from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("shipping_fee") > 16)
    .groupBy("status")
    .agg(F.round(F.stddev("shipping_fee"), 2).alias("stddev_shipping_fee"))
    .filter(F.col("stddev_shipping_fee") >= 53)
    .select("status", "stddev_shipping_fee")
    .orderBy(F.col("stddev_shipping_fee").desc())
)
