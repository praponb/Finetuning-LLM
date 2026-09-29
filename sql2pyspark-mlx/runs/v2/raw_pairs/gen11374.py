from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "left")
    .select(
        F.col("p.order_id"),
        F.col("p.paid_at"),
        F.col("o.customer_id"),
        F.col("o.shipping_fee"),
    )
    .orderBy(F.col("order_id").desc())
)
