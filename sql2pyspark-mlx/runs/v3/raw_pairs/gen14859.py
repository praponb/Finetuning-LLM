from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("p.order_id"), "right")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "inner")
    .filter(F.col("c.segment") == "SMB")
    .groupBy(F.col("c.city"))
    .agg(F.round(F.avg(F.col("p.amount")), 2).alias("avg_amount"))
    .filter(F.col("avg_amount") >= 42)
)
