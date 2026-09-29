from pyspark.sql import functions as F

result = (
    df_orders
    .filter(~((F.col("shipping_fee") >= 16) & (F.col("discount") > 0.13)))
    .select(
        "id",
        "shipping_fee",
        F.concat_ws(",", "channel", "status").alias("channel_status_key"),
    )
)
