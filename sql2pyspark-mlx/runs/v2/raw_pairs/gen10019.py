from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "inner")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "right")
    .select(
        F.col("s.carrier"),
        F.col("s.delivered_date"),
        F.col("o.status"),
        F.col("c.city"),
    )
)
