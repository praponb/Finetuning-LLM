from pyspark.sql import functions as F

result = (
    df_orders
    .filter((F.col("channel") == "store") | F.col("amount").isNull())
    .select(
        "status",
        "shipping_fee",
        "discount",
        F.length("channel").alias("channel_length"),
    )
)
