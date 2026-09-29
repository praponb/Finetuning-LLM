from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("p.order_id"), "left")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "left")
    .select(
        F.col("p.id"),
        F.col("p.amount"),
        F.col("p.method"),
        F.col("o.customer_id"),
        F.col("s.city"),
    )
)
