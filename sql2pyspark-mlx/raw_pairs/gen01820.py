from pyspark.sql import functions as F

src = df_orders.filter((F.col("shipping_fee") <= 35) & (F.col("shipping_fee") <= 24))

orders_stats = (
    src
    .groupBy(
        F.when(F.col("channel").isin(["partner", "web"]), "tier_1").otherwise("other").alias("channel_group"),
        "channel",
    )
    .agg(
        F.min("discount").alias("min_discount"),
        F.max("order_date").alias("last_order_date"),
    )
)

result = (
    orders_stats
    .select("channel_group", "min_discount", "last_order_date")
    .orderBy(F.col("last_order_date").desc(), "channel_group")
)
