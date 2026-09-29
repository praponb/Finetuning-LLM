from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "inner")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "left")
    .select(
        F.col("p.id"),
        F.col("p.amount"),
        F.col("p.status"),
        F.col("o.customer_id"),
        F.col("c.credit_limit"),
    )
    .orderBy(F.col("amount").desc(), F.col("customer_id").desc(), "id")
    .limit(3)
)
