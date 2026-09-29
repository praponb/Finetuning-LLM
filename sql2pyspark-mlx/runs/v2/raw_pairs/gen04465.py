from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "inner")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "inner")
    .groupBy(F.col("c.segment"))
    .agg(
        F.sum(F.when(F.col("p.method") == "bank_transfer", 1).otherwise(0)).alias("bank_transfer_count"),
        F.round(F.avg(F.col("p.amount")), 2).alias("avg_amount"),
    )
    .filter(F.col("avg_amount") >= 42)
)
