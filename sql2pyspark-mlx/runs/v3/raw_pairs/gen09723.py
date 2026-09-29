from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "left")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "inner")
    .filter(F.col("p.order_id").isNotNull())
    .groupBy(
        F.when(F.col("s.region").isin(["West", "South"]), "tier_1").otherwise("other").alias("region_group"),
    )
    .agg(
        F.max(F.col("p.paid_at")).alias("latest_paid_at"),
        F.round(F.stddev(F.col("p.amount")), 2).alias("stddev_amount"),
    )
)
