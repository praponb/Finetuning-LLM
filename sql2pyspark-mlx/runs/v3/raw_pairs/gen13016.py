from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("channel") == "partner")
    .select(
        "store_id",
        "customer_id",
        "status",
        (F.col("shipping_fee") + F.col("amount")).alias("amount_with_shipping"),
    )
)
