from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "left")
    .filter(F.col("p.status") == "REFUNDED")
    .select(
        F.col("p.paid_at"),
        F.col("p.status"),
        F.col("p.order_id"),
        F.col("o.status").alias("order_status"),
        F.col("o.order_date"),
        F.col("o.shipping_fee"),
    )
    .orderBy("order_date")
)
