from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "inner")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "inner")
    .groupBy(F.col("c.segment"), F.col("c.is_active"))
    .agg(
        F.min(F.col("p.amount")).alias("min_amount"),
        F.avg(F.col("p.amount")).alias("avg_amount"),
    )
    .orderBy(F.col("avg_amount").desc())
)
