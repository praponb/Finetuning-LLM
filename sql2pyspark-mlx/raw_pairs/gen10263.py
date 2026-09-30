from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("amount").between(510, 1370))
    .select(
        "channel",
        "status",
        "shipping_fee",
        F.round((F.col("discount") / F.col("amount")), 2).alias("discount_per_amount"),
    )
)
