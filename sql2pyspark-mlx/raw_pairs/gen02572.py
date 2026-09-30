from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("store_id").isNull())
    .groupBy("status", F.month("order_date").alias("order_month"))
    .agg(
        F.count("shipping_fee").alias("shipping_fee_count"),
        F.min("shipping_fee").alias("min_shipping_fee"),
    )
)
