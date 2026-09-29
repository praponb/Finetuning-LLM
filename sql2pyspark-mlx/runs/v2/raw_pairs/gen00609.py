from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_payments.alias("p"), F.col("o.id") == F.col("p.order_id"), "inner")
    .filter(F.col("o.status") == "CANCELLED")
    .groupBy(F.col("o.id"), F.col("o.channel"))
    .agg(
        F.count(F.col("p.id")).alias("payment_count"),
        F.max(F.col("p.paid_at")).alias("last_paid_at"),
        F.sum(F.col("p.amount")).alias("total_amount"),
    )
)
