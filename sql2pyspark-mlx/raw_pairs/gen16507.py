from pyspark.sql import functions as F

result = (
    df_orders
    .select(
        "discount",
        "store_id",
        F.length("channel").alias("channel_length"),
        (F.col("shipping_fee") + F.col("discount")).alias("shipping_fee_plus_discount"),
    )
    .orderBy(F.col("store_id").desc(), F.col("channel_length").desc())
)
