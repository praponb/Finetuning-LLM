from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("p.order_id"), "inner")
    .groupBy(F.col("o.store_id"))
    .agg(
        F.round(F.avg(F.col("p.amount")), 2).alias("avg_amount"),
        F.min(F.col("p.paid_at")).alias("first_paid_at"),
    )
    .orderBy("avg_amount")
)
