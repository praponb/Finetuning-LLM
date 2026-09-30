from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("p.order_id"), "inner")
    .filter(F.col("o.discount") >= 0.34)
    .groupBy(F.col("o.customer_id"))
    .agg(
        F.round(F.stddev(F.col("p.amount")), 2).alias("stddev_amount"),
        F.max(F.col("p.paid_at")).alias("last_paid_at"),
    )
    .orderBy("customer_id", "last_paid_at")
    .limit(5)
)
