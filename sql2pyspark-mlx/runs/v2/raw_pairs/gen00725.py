from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("status") == "CANCELLED")
    .groupBy(F.month("order_date").alias("order_month"))
    .agg(
        F.sort_array(F.collect_set("channel")).alias("channel_list"),
        F.round(F.avg("amount"), 2).alias("avg_amount"),
        F.sum(F.when(F.col("channel") == "mobile", F.col("shipping_fee")).otherwise(0)).alias("mobile_shipping_fee"),
    )
    .filter(F.col("mobile_shipping_fee") > 38)
    .drop("mobile_shipping_fee")
    .orderBy(F.col("order_month").desc())
)
