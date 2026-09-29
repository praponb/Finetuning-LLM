from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("shipping_fee") > 24)
    .select(
        "id",
        "shipping_fee",
        "channel",
        F.coalesce("shipping_fee", F.lit(0)).alias("shipping_fee_filled"),
    )
    .orderBy(F.col("id").desc())
)
