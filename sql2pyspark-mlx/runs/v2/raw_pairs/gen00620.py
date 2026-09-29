from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy("channel")
    .agg(
        F.min("shipping_fee").alias("min_shipping_fee"),
        F.min("amount").alias("min_amount"),
    )
)
