from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("discount") <= 0.35)
    .groupBy("status", F.date_format("order_date", "yyyy-MM").alias("order_month"))
    .agg(
        F.sort_array(F.collect_set("channel")).alias("channel_list"),
        F.min("shipping_fee").alias("min_shipping_fee"),
    )
    .filter(F.col("min_shipping_fee") >= 29)
    .drop("min_shipping_fee")
)
