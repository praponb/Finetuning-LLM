from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "left")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("o.customer_id"), "inner")
    .filter((F.col("s.delivered_date") < "2024-11-27") | F.col("c.name").like("Al%"))
    .select(
        F.col("s.id"),
        F.col("s.status"),
        F.col("o.order_date"),
        F.col("o.amount"),
        F.col("c.email"),
        F.substring(F.col("o.channel"), 1, 4).alias("channel_prefix"),
    )
    .orderBy(F.col("email").desc(), "order_date")
)
