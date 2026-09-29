from pyspark.sql import functions as F

result = (
    df_orders
    .select(
        "customer_id",
        "store_id",
        (F.col("discount") * F.col("shipping_fee")).alias("discount_times_shipping_fee"),
        F.concat_ws(",", "status", "channel").alias("status_channel_key"),
    )
    .orderBy(F.col("discount_times_shipping_fee").desc())
)
