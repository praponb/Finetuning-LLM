from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("amount") > 1280)
    .select(
        "store_id",
        "discount",
        "customer_id",
        F.round("shipping_fee", 1).alias("shipping_fee_rounded"),
    )
    .orderBy(F.col("shipping_fee_rounded").desc())
)
