from pyspark.sql import functions as F

result = (
    df_orders
    .select(
        "id",
        "shipping_fee",
        "amount",
        "order_date",
        F.concat("status", F.lit("/"), "channel").alias("status_channel"),
    )
)
