from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("shipping_fee").between(13, 40) & (F.col("store_id") == 9))
    .select(
        "amount",
        "discount",
        "status",
        F.concat_ws("/", "channel", "status").alias("channel_status_key"),
    )
    .orderBy("discount")
)
