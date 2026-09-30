from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "left")
    .filter(F.col("p.order_id").isNull())
    .groupBy(
        F.col("o.status"),
        F.when(F.col("o.channel").isin(["store", "web"]), "core").otherwise("other").alias("channel_group"),
    )
    .agg(F.round(F.avg(F.col("p.amount")), 2).alias("avg_amount"))
    .orderBy(F.col("status").desc(), "channel_group")
    .limit(5)
)
