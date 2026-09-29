from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("amount") <= 1350)
    .select(
        "id",
        "amount",
        "store_id",
        (F.col("amount") + F.col("shipping_fee")).alias("amount_with_shipping"),
    )
)
