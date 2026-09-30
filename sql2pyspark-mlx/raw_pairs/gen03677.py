from pyspark.sql import functions as F

result = (
    df_orders
    .filter((F.col("shipping_fee") <= 28) & ((F.col("discount") < 0.16) | F.col("discount").between(0.17, 0.37)))
    .select(
        "id",
        "status",
        F.round("amount", 1).alias("amount_rounded"),
        F.round("shipping_fee", 1).alias("shipping_fee_rounded"),
    )
    .orderBy(F.col("amount_rounded").desc())
)
