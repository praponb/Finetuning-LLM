from pyspark.sql import functions as F

result = (
    df_orders
    .filter(((F.col("customer_id") == 10) & (F.col("channel") == "partner")) | F.col("customer_id").isin([6, 11, 36]))
    .groupBy("status", F.quarter("order_date").alias("order_quarter"))
    .agg(
        F.min("shipping_fee").alias("min_shipping_fee"),
        F.round(F.avg("amount"), 2).alias("avg_amount"),
    )
)
