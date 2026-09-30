from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "inner")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "outer")
    .select(
        F.col("p.id"),
        F.col("p.paid_at"),
        F.col("o.order_date"),
        F.col("o.customer_id"),
        F.col("s.opened_date"),
        F.col("s.region"),
    )
    .orderBy("customer_id")
)
