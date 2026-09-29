from pyspark.sql import functions as F

result = (
    df_orders
    .filter((F.month("order_date").isin([3, 9, 10]) & (F.col("shipping_fee") > 23)) | F.col("amount").between(1170, 1470))
    .select(
        "store_id",
        "shipping_fee",
        F.concat("channel", F.lit("/"), "status").alias("channel_status"),
    )
)
