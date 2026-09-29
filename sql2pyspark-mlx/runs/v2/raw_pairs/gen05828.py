from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy("status", "store_id")
    .agg(
        (F.max("shipping_fee") - F.min("shipping_fee")).alias("shipping_fee_range"),
        F.sum(F.when(F.col("channel") == "partner", F.col("shipping_fee")).otherwise(0)).alias("partner_shipping_fee"),
    )
    .orderBy("partner_shipping_fee", F.col("store_id").desc())
)
