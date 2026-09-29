from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.month("order_date").isin([2, 4, 9]) & F.col("store_id").isNull())
    .groupBy(F.date_trunc("MONTH", "order_date").alias("order_month"), "customer_id")
    .agg(
        F.round(F.stddev("shipping_fee"), 2).alias("stddev_shipping_fee"),
        F.max("discount").alias("max_discount"),
    )
    .filter(F.col("stddev_shipping_fee") < 39)
)
