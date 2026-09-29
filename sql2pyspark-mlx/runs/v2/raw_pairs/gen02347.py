from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("shipping_fee") > 34)
    .select(
        "id",
        "status",
        "channel",
        "shipping_fee",
        F.when(F.col("discount") >= 0.36, "High").when(F.col("discount") >= 0.14, "Medium").otherwise("Low").alias("discount_level"),
        (F.col("amount") - F.col("shipping_fee")).alias("amount_with_shipping"),
    )
    .orderBy("status")
)
