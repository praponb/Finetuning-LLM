from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy(F.date_format("order_date", "yyyy-MM").alias("order_month"), "customer_id")
    .agg(
        F.sum("shipping_fee").alias("total_shipping_fee"),
        F.min("discount").alias("min_discount"),
    )
    .orderBy(F.col("min_discount").desc(), "order_month", "customer_id")
    .limit(10)
)
