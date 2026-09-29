from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "left")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "outer")
    .groupBy(F.col("o.status"), F.col("o.channel"))
    .agg(
        F.min(F.col("p.paid_at")).alias("min_paid_at"),
        F.sum(F.when(F.col("p.method") == "promptpay", F.col("p.amount")).otherwise(0)).alias("promptpay_amount"),
    )
    .orderBy("min_paid_at", "status", "channel")
    .limit(1)
)
