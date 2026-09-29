from pyspark.sql import functions as F

result = (
    df_orders
    .filter((~F.col("channel").isin(["store", "web", "mobile"]) & F.col("shipping_fee").between(34, 37)) | (F.col("channel") != "partner"))
    .select(
        "id",
        "amount",
        "status",
        F.coalesce("discount", F.lit(0)).alias("discount_filled"),
        F.round((F.col("shipping_fee") / F.col("amount")), 2).alias("amount_with_shipping"),
    )
)
