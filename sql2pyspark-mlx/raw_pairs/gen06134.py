from pyspark.sql import functions as F

recent_orders = (
    df_orders
    .filter(F.col("customer_id").isNotNull() & F.col("shipping_fee").isNotNull())
)

agg = (
    recent_orders
    .groupBy(F.date_trunc("MONTH", "order_date").alias("order_month"), "channel")
    .agg(
        F.min("order_date").alias("min_order_date"),
        F.round(F.stddev("shipping_fee"), 2).alias("stddev_shipping_fee"),
    )
)

result = agg.select("order_month", "channel", "min_order_date", "stddev_shipping_fee")
