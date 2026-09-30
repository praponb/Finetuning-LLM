from pyspark.sql import functions as F

result = (
    df_orders
    .select(
        "id",
        "status",
        "shipping_fee",
        "store_id",
        F.concat_ws("-", "channel", "status").alias("channel_status_key"),
    )
)
