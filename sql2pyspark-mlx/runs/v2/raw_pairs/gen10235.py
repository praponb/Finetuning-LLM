from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_payments.alias("p"), F.col("p.order_id") == F.col("o.id"), "left")
    .groupBy(F.col("o.id"), F.col("o.status"))
    .agg(
        F.count(F.col("p.id")).alias("payment_count"),
        F.max(F.col("p.paid_at")).alias("latest_paid_at"),
        F.min(F.col("p.paid_at")).alias("first_paid_at"),
    )
)
