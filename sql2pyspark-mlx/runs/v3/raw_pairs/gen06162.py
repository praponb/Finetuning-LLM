from pyspark.sql import functions as F

result = (
    df_orders
    .filter((F.col("amount") > 470) & F.col("shipping_fee").isNotNull())
    .select(
        "channel",
        "discount",
        "status",
        F.when(F.col("shipping_fee") >= 34, "High").when(F.col("shipping_fee") >= 8, "Medium").otherwise("Low").alias("shipping_fee_tier"),
    )
    .orderBy(F.col("shipping_fee_tier").desc(), "discount")
)
