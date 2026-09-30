from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "inner")
    .filter((F.col("c.signup_date") < "2025-03-01") & F.col("o.shipping_fee").between(21, 25))
    .select(
        F.col("o.id"),
        F.col("o.customer_id"),
        F.col("o.store_id"),
        F.col("o.amount"),
        F.col("c.city"),
    )
)
