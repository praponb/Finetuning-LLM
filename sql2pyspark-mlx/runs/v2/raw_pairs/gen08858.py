from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "left")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "inner")
    .filter(F.col("o.channel").isin(["partner", "web", "store"]) & (F.col("c.age") < 65))
    .select(
        F.col("s.order_id"),
        F.col("s.status"),
        F.col("o.store_id"),
        F.col("o.customer_id"),
        F.col("c.segment"),
        F.col("c.city"),
    )
)
