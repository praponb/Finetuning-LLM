from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "inner")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "left")
    .filter(F.col("s.shipped_date") >= "2025-09-01")
    .select(
        F.col("s.id"),
        F.col("s.delivered_date"),
        F.col("o.order_date"),
        F.col("o.channel"),
        F.col("c.email"),
        F.col("c.name"),
    )
)
