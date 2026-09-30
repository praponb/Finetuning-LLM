from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("s.order_id"), "inner")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "inner")
    .filter(F.col("c.is_active") | (F.col("s.cost") >= 30))
    .select(
        F.col("s.id"),
        F.col("s.shipped_date"),
        F.col("o.store_id"),
        F.col("o.order_date"),
        F.col("c.age"),
        F.col("c.city"),
    )
)
