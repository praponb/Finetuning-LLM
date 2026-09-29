from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("o.store_id"), "left")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("o.customer_id"), "left")
    .filter(F.col("o.amount").between(1280, 1350) & (F.col("o.order_date") < "2025-05-01"))
    .select(
        F.col("o.id"),
        F.col("o.amount"),
        F.col("o.store_id"),
        F.col("s.region"),
        F.col("s.city"),
        F.col("c.email"),
    )
)
