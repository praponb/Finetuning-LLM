from pyspark.sql import functions as F

valid_orders = (
    df_orders
    .filter(F.col("status") == "COMPLETED")
    .withColumn("shipping_fee_int", F.col("shipping_fee").cast("int"))
)

status_summary = (
    valid_orders
    .groupBy("status")
    .agg(
        F.max("order_date").alias("latest_order_date"),
        F.round(F.stddev("shipping_fee"), 2).alias("stddev_shipping_fee"),
    )
)

result = status_summary.select("status", "latest_order_date", "stddev_shipping_fee")
