from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy("status")
    .agg(
        (F.max("shipping_fee") - F.min("shipping_fee")).alias("shipping_fee_range"),
        F.sum(F.when(F.col("channel") == "web", F.col("shipping_fee")).otherwise(0)).alias("web_shipping_fee"),
    )
    .filter(F.col("web_shipping_fee") > 64)
    .drop("web_shipping_fee")
)
