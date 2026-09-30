from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_payments.alias("p"), F.col("p.order_id") == F.col("o.id"), "left")
    .filter(F.col("o.store_id").isNotNull())
    .groupBy(F.col("o.channel"))
    .agg(
        F.count(F.col("p.id")).alias("payment_count"),
        F.max(F.col("p.paid_at")).alias("latest_paid_at"),
    )
    .orderBy("payment_count")
)
