from pyspark.sql import functions as F

result = (
    df_payments
    .select("amount")
    .unionByName(
        df_orders
        .filter(F.col("discount").between(0.17, 0.18))
        .select(F.col("shipping_fee").alias("amount"))
    )
    .distinct()
)
