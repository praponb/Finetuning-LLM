from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "left")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("o.customer_id"), "inner")
    .filter(F.col("s.shipped_date").between("2025-05-23", "2025-06-17") & (F.col("o.discount") >= 0.18))
    .select(
        F.col("s.order_id"),
        F.col("o.discount"),
        F.col("c.segment"),
        F.col("c.city"),
    )
)
