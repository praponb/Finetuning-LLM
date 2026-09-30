from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "inner")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "inner")
    .select(
        F.col("s.id"),
        F.col("s.order_id"),
        F.col("o.order_date"),
        F.col("o.customer_id"),
        F.col("c.email"),
        F.col("c.credit_limit"),
    )
)
