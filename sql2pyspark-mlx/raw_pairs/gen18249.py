from pyspark.sql import functions as F

result = (
    df_orders
    .filter((F.col("amount") < 510) | F.col("status").isin(["COMPLETED", "CANCELLED"]))
    .select(
        "id",
        "discount",
        "shipping_fee",
        F.concat("channel", F.lit(" "), "status").alias("channel_status"),
        F.greatest("shipping_fee", "discount").alias("greatest_shipping_fee_discount"),
    )
)
