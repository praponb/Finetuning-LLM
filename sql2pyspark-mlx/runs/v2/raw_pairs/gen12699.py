from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy("channel")
    .agg(
        F.max("amount").alias("max_amount"),
        (F.max("shipping_fee") - F.min("shipping_fee")).alias("shipping_fee_range"),
        F.sum("shipping_fee").alias("total_shipping_fee"),
    )
    .filter(F.col("total_shipping_fee") >= 120)
    .drop("total_shipping_fee")
    .orderBy(F.col("shipping_fee_range").desc(), "max_amount", "channel")
    .limit(10)
)
