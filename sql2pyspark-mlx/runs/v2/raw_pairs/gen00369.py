from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy("channel", F.date_format("order_date", "yyyy-MM").alias("order_month"))
    .agg(
        (F.max("amount") - F.min("amount")).alias("amount_range"),
        F.sum("shipping_fee").alias("total_shipping_fee"),
    )
    .orderBy("total_shipping_fee", "channel", "order_month")
    .limit(50)
)
