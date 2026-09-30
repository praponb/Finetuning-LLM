from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "left")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "inner")
    .filter(F.col("o.status") == "PENDING")
    .groupBy(F.col("o.status"))
    .agg(F.min(F.col("p.paid_at")).alias("first_paid_at"))
    .orderBy(F.col("status").desc())
)
