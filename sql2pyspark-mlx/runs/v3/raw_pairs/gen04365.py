from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("channel") == "store")
    .select(
        "shipping_fee",
        "status",
        "amount",
        (F.col("amount") + F.col("discount")).alias("amount_plus_discount"),
        F.initcap(F.trim("channel")).alias("channel_title"),
    )
)
