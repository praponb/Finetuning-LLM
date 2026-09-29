from pyspark.sql import functions as F

result = (
    df_orders
    .select(
        "status",
        "amount",
        F.greatest("discount", "shipping_fee").alias("greatest_discount_shipping_fee"),
    )
    .orderBy(F.col("amount").desc())
)
