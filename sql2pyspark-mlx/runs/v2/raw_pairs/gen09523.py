from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "inner")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "left")
    .filter(F.col("c.name").isNotNull())
    .groupBy(F.col("c.city"), F.col("c.country"))
    .agg(F.count(F.col("p.amount")).alias("amount_count"))
)
