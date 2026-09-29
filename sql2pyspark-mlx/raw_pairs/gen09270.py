from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("s.order_id"), "left")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "inner")
    .select(
        F.col("s.order_id"),
        F.col("o.amount"),
        F.col("o.customer_id"),
        F.col("c.credit_limit"),
        F.col("c.segment"),
    )
)
