from pyspark.sql import functions as F

result = (
    df_orders
    .select(
        "id",
        "amount",
        F.when(F.col("discount") >= 0.38, "High").when(F.col("discount") >= 0.05, "Medium").otherwise("Low").alias("discount_level"),
        (F.col("shipping_fee") * F.col("amount")).alias("amount_with_shipping"),
    )
    .orderBy(F.col("amount_with_shipping").desc(), F.col("id").desc())
    .limit(1)
)
