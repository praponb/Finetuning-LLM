from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "inner")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("o.customer_id"), "inner")
    .groupBy(F.col("c.segment"))
    .agg(
        F.round(F.stddev(F.col("p.amount")), 2).alias("stddev_amount"),
        F.min(F.col("p.paid_at")).alias("min_paid_at"),
    )
    .orderBy("segment")
)
