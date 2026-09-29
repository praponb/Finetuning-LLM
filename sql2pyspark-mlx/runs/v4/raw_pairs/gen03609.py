from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "left")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("o.customer_id"), "left")
    .groupBy(F.col("o.status"))
    .agg(F.sum(F.col("p.amount")).alias("total_amount"))
    .filter(F.col("total_amount") >= 3000)
)
