from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("customer_id").isNotNull() | F.col("channel").isNull())
    .select(
        "id",
        "shipping_fee",
        "discount",
        "customer_id",
        (F.col("discount") - F.col("amount")).alias("discount_minus_amount"),
    )
    .orderBy(F.col("shipping_fee").desc(), F.col("discount").desc())
)
