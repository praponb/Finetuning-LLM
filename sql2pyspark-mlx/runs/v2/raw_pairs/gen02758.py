from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("s.order_id"), "left")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "left")
    .select(
        F.col("s.id"),
        F.col("s.shipped_date"),
        F.col("o.store_id"),
        F.col("o.customer_id"),
        F.col("c.email"),
        F.substring(F.col("c.email"), 1, 3).alias("email_prefix"),
    )
)
