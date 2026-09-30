from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "left")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "inner")
    .filter((F.col("o.order_date") > "2023-08-01") & (F.col("o.discount") <= 0.25))
    .select(
        F.col("p.order_id"),
        F.col("p.paid_at"),
        F.col("o.shipping_fee"),
        F.col("c.country"),
        F.col("c.is_active"),
        F.round((F.col("c.age") / F.col("c.credit_limit")), 2).alias("age_per_credit_limit"),
    )
)
