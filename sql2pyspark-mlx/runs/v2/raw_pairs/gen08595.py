from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "left")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "left")
    .select(
        F.col("s.id"),
        F.col("s.carrier"),
        F.col("s.order_id"),
        F.col("o.order_date"),
        F.col("o.status"),
        F.col("c.age"),
        F.col("c.email"),
    )
    .orderBy("carrier", "id")
    .limit(3)
)
