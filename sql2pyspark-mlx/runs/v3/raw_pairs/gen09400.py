from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("discount").between(0.13, 0.31))
    .select(
        "store_id",
        "status",
        "shipping_fee",
        F.date_add("order_date", 65).alias("order_date_plus_65d"),
        F.length("channel").alias("channel_length"),
    )
)
