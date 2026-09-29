from pyspark.sql import functions as F

result = (
    df_orders
    .select(
        "shipping_fee",
        "channel",
        F.round((F.col("amount") * 0.85), 2).alias("amount_scaled"),
    )
)
