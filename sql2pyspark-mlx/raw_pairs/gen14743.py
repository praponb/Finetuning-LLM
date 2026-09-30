from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("discount").between(0.2, 0.36))
    .groupBy(F.date_trunc("MONTH", "order_date").alias("order_month"))
    .agg(
        F.sum("shipping_fee").alias("total_shipping_fee"),
        F.min("shipping_fee").alias("min_shipping_fee"),
    )
    .filter(F.col("total_shipping_fee") >= 130)
    .orderBy("min_shipping_fee", "order_month")
)
