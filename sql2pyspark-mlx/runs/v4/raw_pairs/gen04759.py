from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "left")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "left")
    .filter(~((F.col("p.paid_at") < "2024-05-03") & F.hour(F.col("p.paid_at")).between(10, 18)))
    .select(F.col("p.amount"), F.col("o.status"), F.col("c.age"), F.col("c.city"))
)
