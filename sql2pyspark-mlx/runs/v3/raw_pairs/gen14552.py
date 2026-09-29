from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "inner")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "inner")
    .filter(F.col("p.method").isin(["card", "cash", "promptpay"]))
    .groupBy(
        F.when(F.col("o.channel").isin(["store", "web"]), "tier_1").otherwise("other").alias("channel_group"),
        F.col("o.status"),
    )
    .agg((F.max(F.col("p.amount")) - F.min(F.col("p.amount"))).alias("amount_range"))
)
