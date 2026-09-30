from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("status") == "COMPLETED")
    .groupBy(F.quarter("order_date").alias("order_quarter"))
    .agg(
        F.round(F.stddev("discount"), 2).alias("stddev_discount"),
        F.sort_array(F.collect_set("channel")).alias("channel_list"),
        F.max("shipping_fee").alias("max_shipping_fee"),
    )
    .filter(F.col("max_shipping_fee") < 13)
    .drop("max_shipping_fee")
    .orderBy(F.col("stddev_discount").desc())
)
