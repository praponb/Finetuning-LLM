from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("p.order_id"), "left")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "outer")
    .groupBy(F.col("o.status"), F.col("o.channel"))
    .agg(
        F.max(F.col("p.paid_at")).alias("latest_paid_at"),
        F.round(F.stddev(F.col("p.amount")), 2).alias("stddev_amount"),
    )
    .orderBy("latest_paid_at")
)
