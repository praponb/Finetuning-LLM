from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.month("order_date").isin([2, 9, 12]) & ((F.col("channel") == "mobile") | F.col("customer_id").isNull()))
    .groupBy("status")
    .agg(
        F.max("order_date").alias("latest_order_date"),
        F.sum(F.when(F.col("channel") == "mobile", F.col("amount")).otherwise(0)).alias("mobile_amount"),
        F.min("shipping_fee").alias("min_shipping_fee"),
    )
)
