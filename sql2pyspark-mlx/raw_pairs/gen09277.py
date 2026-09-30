from pyspark.sql import functions as F

result = (
    df_orders
    .select(
        "id",
        "amount",
        "channel",
        F.when(F.col("shipping_fee") >= 44, "High").when(F.col("shipping_fee") >= 10, "Medium").otherwise("Low").alias("shipping_fee_level"),
    )
)
