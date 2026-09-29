from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("p.order_id"), "left")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "left")
    .filter(F.col("o.status").isin(["CANCELLED", "RETURNED", "COMPLETED"]))
    .groupBy(F.quarter(F.col("o.order_date")).alias("order_quarter"), F.col("o.status"))
    .agg(F.max(F.col("p.paid_at")).alias("last_paid_at"))
)
