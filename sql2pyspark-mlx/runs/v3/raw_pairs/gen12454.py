from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("o.customer_id"), "right")
    .filter(F.col("o.shipping_fee") <= 35)
    .select(
        F.col("o.discount"),
        F.col("o.store_id"),
        F.col("c.is_active"),
        F.col("c.age"),
        F.col("c.email"),
        F.date_add(F.col("c.signup_date"), 31).alias("signup_date_plus_31d"),
    )
)
