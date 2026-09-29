from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "left")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("o.customer_id"), "inner")
    .filter((F.col("s.order_id") == 14) & F.col("o.order_date").between("2024-09-01", "2025-03-24"))
    .select(
        F.col("s.id"),
        F.col("s.shipped_date"),
        F.col("o.discount"),
        F.col("o.shipping_fee"),
        F.col("c.segment"),
        F.col("c.age"),
    )
)
