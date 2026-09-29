from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("channel").isin(["store", "mobile"]))
    .select(
        "id",
        "channel",
        "amount",
        "shipping_fee",
        (F.datediff(F.current_date(), "order_date") / 30).alias("months_since_order_date"),
    )
)
