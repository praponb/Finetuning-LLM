from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("p.order_id"), "right")
    .groupBy(F.col("o.status"), F.col("o.customer_id"))
    .agg(
        (F.max(F.col("p.amount")) - F.min(F.col("p.amount"))).alias("amount_range"),
        F.round(F.avg(F.col("p.amount")), 2).alias("avg_amount"),
    )
    .orderBy("avg_amount", "status", "customer_id")
    .limit(10)
)
