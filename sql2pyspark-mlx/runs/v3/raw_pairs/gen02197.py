from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "inner")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "outer")
    .filter(F.year(F.col("c.signup_date")) == 2025)
    .select(
        F.col("s.carrier"),
        F.col("o.store_id"),
        F.col("c.signup_date"),
        F.col("c.email"),
    )
)
