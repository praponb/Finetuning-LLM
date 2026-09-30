from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "left")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "inner")
    .filter(F.col("o.discount") < 0.19)
    .groupBy(F.col("c.city"))
    .agg(F.round(F.avg(F.col("p.amount")), 2).alias("avg_amount"))
    .orderBy(F.col("avg_amount").desc())
)
