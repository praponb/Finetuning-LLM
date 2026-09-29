from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("shipping_fee") < 21)
    .select(
        "id",
        "status",
        "discount",
        "shipping_fee",
        F.when(F.col("discount") >= 0.32, "High").when(F.col("discount") >= 0.14, "Medium").otherwise("Low").alias("discount_tier"),
        F.coalesce("shipping_fee", F.lit(0)).alias("shipping_fee_filled"),
    )
    .orderBy("status")
)
