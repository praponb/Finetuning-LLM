from pyspark.sql import functions as F

recent_orders = (
    df_orders
    .filter(F.col("channel").isin(["store", "partner", "web"]) | F.col("discount").between(0.24, 0.35))
)

channel_summary = (
    recent_orders
    .groupBy("channel")
    .agg(
        F.sum(F.when(F.col("channel") == "mobile", F.col("shipping_fee")).otherwise(0)).alias("mobile_shipping_fee"),
    )
)

result = (
    channel_summary
    .filter(F.col("mobile_shipping_fee") > 60)
    .select("channel", "mobile_shipping_fee")
    .orderBy(F.col("mobile_shipping_fee").desc(), "channel")
    .limit(10)
)
