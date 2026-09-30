from pyspark.sql import functions as F

result = (
    df_orders
    .select(
        "id",
        "channel",
        (F.col("shipping_fee") - F.col("discount")).alias("shipping_fee_minus_discount"),
    )
)
