from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.year("order_date") >= 2023)
    .select(
        "id",
        "channel",
        "status",
        F.round("shipping_fee", 0).alias("shipping_fee_rounded"),
    )
)
