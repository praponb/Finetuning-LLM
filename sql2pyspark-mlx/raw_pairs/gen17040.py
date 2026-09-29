from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "inner")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "inner")
    .select(
        F.col("p.paid_at"),
        F.col("o.status"),
        F.col("c.signup_date"),
        F.col("c.credit_limit"),
    )
    .orderBy(F.col("credit_limit").desc())
)
