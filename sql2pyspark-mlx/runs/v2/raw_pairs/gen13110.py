from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "inner")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "inner")
    .filter(F.col("p.status") == "REFUNDED")
    .groupBy(F.col("o.status"), F.month(F.col("o.order_date")).alias("order_month"))
    .agg(
        F.max(F.col("p.paid_at")).alias("last_paid_at"),
        F.round(F.avg(F.col("p.amount")), 2).alias("avg_amount"),
    )
)
