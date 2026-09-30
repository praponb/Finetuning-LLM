from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy("status")
    .agg(
        F.max("shipping_fee").alias("max_shipping_fee"),
        F.max("order_date").alias("latest_order_date"),
    )
    .filter(F.col("max_shipping_fee") > 11)
)
