from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "right")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "inner")
    .filter(F.hour(F.col("p.paid_at")).between(12, 16))
    .groupBy(F.quarter(F.col("o.order_date")).alias("order_quarter"))
    .agg(F.round(F.avg(F.col("p.amount")), 2).alias("avg_amount"))
    .filter(F.col("avg_amount") >= 61)
)
