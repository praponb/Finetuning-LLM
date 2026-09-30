from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("s.order_id"), "right")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "inner")
    .filter(F.col("o.status") == "SHIPPED")
    .select(
        F.col("s.id"),
        F.col("s.shipped_date"),
        F.col("s.order_id"),
        F.col("o.shipping_fee"),
        F.col("o.customer_id"),
        F.col("c.city"),
    )
    .orderBy("shipped_date")
)
