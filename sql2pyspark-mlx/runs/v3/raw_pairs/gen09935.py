from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "inner")
    .filter(F.col("o.discount") > 0.29)
    .groupBy(F.col("o.status"))
    .agg(
        F.min(F.col("p.paid_at")).alias("first_paid_at"),
        F.avg(F.col("p.amount")).alias("avg_amount"),
    )
    .orderBy("status")
    .limit(50)
)
