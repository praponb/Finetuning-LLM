from pyspark.sql import functions as F

result = (
    df_orders
    .filter((F.col("status") == "PENDING") & ((F.col("shipping_fee") >= 12) | (F.col("amount") < 1340)))
    .select(
        "customer_id",
        F.floor("discount").alias("discount_whole"),
        (F.col("discount") / F.nullif("shipping_fee", F.lit(0))).alias("discount_per_shipping_fee"),
    )
    .orderBy(F.col("discount_per_shipping_fee").desc())
)
