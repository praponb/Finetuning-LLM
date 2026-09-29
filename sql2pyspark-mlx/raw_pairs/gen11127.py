from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("p.order_id"), "outer")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("o.customer_id"), "inner")
    .filter(~(F.col("c.email").isNotNull() & (F.col("o.shipping_fee") > 20)))
    .select(
        F.col("p.id"),
        F.col("p.method"),
        F.col("p.amount"),
        F.col("o.status"),
        F.col("c.signup_date"),
        F.col("c.city"),
    )
)
