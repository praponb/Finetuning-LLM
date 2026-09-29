from pyspark.sql import functions as F

result = (
    df_orders
    .select(
        "id",
        "order_date",
        "status",
        (F.col("amount") * F.col("shipping_fee")).alias("amount_with_shipping"),
        F.round((F.col("shipping_fee") / F.col("amount")), 2).alias("amount_with_shipping_2"),
    )
    .orderBy(F.col("amount_with_shipping_2").desc())
)
