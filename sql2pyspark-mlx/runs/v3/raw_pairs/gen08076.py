from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("channel").isin(["partner", "store"]))
    .select(
        "id",
        "channel",
        "amount",
        F.round("shipping_fee", 2).alias("shipping_fee_rounded"),
        (F.col("amount") - F.col("shipping_fee")).alias("amount_with_shipping"),
    )
)
