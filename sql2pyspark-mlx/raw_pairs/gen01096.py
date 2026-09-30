from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("shipping_fee").isNotNull())
    .select("shipping_fee")
    .intersect(
        df_payments
        .filter(F.col("amount").isNotNull())
        .select(F.col("amount").alias("shipping_fee"))
    )
    .orderBy("shipping_fee")
)
