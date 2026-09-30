from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("p.order_id"), "left")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "left")
    .select(
        F.col("p.id"),
        F.col("p.status"),
        F.col("o.shipping_fee"),
        F.col("o.customer_id"),
        F.col("c.signup_date"),
    )
    .orderBy(F.col("shipping_fee").desc(), F.col("status").desc(), "id")
    .limit(1)
)
