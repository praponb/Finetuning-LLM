from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy("status", F.month("order_date").alias("order_month"))
    .agg(
        F.count("*").alias("num_orders"),
        F.sum(F.when(F.col("channel") == "mobile", 1).otherwise(0)).alias("mobile_count"),
        F.avg("shipping_fee").alias("avg_shipping_fee"),
    )
    .filter(F.col("avg_shipping_fee") < 24)
    .drop("avg_shipping_fee")
    .orderBy(F.col("num_orders").desc())
)
