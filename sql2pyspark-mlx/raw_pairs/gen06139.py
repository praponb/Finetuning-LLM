from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("p.order_id"), "left")
    .filter(F.col("o.shipping_fee") > 31)
    .groupBy(F.col("o.channel"), F.col("o.status"))
    .agg(
        F.min(F.col("p.paid_at")).alias("min_paid_at"),
        F.avg(F.col("p.amount")).alias("avg_amount"),
    )
    .orderBy("channel")
)
