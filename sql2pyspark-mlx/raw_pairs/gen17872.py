from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("p.order_id"), "inner")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "inner")
    .filter(F.col("p.amount").between(1310, 1540))
    .groupBy(F.col("c.segment"))
    .agg(F.round(F.stddev(F.col("p.amount")), 2).alias("stddev_amount"))
    .orderBy(F.col("stddev_amount").desc(), "segment")
)
