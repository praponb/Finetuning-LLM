from pyspark.sql import functions as F

result = (
    df_orders
    .filter(((F.col("amount") < 1060) & (F.col("order_date") >= "2023-09-01")) | (F.col("customer_id") == 8))
    .select(
        "status",
        "channel",
        "customer_id",
        (F.col("discount") / F.nullif("shipping_fee", F.lit(0))).alias("discount_per_shipping_fee"),
    )
)
