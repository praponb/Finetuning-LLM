from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "outer")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "left")
    .filter(F.col("o.discount") >= 0.16)
    .groupBy(F.col("c.city"))
    .agg(
        F.sum(F.col("p.amount")).alias("total_amount"),
        F.round(F.stddev(F.col("p.amount")), 2).alias("stddev_amount"),
    )
    .filter(F.col("stddev_amount") >= 42)
    .orderBy("total_amount")
)
