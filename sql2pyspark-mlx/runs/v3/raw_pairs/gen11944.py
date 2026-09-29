from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy("channel")
    .agg(
        F.max("shipping_fee").alias("max_shipping_fee"),
        F.min("order_date").alias("min_order_date"),
    )
    .filter(F.col("max_shipping_fee") >= 39)
    .orderBy("min_order_date")
)
