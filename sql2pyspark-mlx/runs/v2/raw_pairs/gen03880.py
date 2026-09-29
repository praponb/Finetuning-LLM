from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "inner")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("o.customer_id"), "inner")
    .select(F.col("p.amount"), F.col("o.status"), F.col("c.name"), F.col("c.country"))
    .orderBy(F.col("country").desc(), F.col("amount").desc())
)
