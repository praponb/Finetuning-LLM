from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "outer")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("o.store_id"), "left")
    .filter(F.col("p.status") != "REFUNDED")
    .select(
        F.col("p.method"),
        F.col("p.paid_at"),
        F.col("o.customer_id"),
        F.col("o.shipping_fee"),
        F.col("s.region"),
    )
)
