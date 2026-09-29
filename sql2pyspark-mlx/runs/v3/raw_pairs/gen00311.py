from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("shipping_fee").isNotNull())
    .select(
        "status",
        "amount",
        (F.col("amount") * F.col("shipping_fee")).alias("amount_with_shipping"),
    )
)
