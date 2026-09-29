from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "inner")
    .groupBy(
        F.col("o.channel"),
        F.when(F.col("o.channel").isin(["mobile", "store"]), "tier_1").otherwise("other").alias("channel_group"),
    )
    .agg(
        F.round(((100 * F.sum(F.when(F.col("s.carrier") == "UPS", 1).otherwise(0))) / F.count("*")), 1).alias("pct_ups"),
    )
    .orderBy("pct_ups", "channel_group")
)
