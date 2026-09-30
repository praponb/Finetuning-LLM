from pyspark.sql import functions as F

result = (
    df_orders.alias("orders")
    .join(df_payments.alias("payments"), F.col("orders.id") == F.col("payments.order_id"), "inner")
    .select(
        F.col("orders.discount"),
        F.col("orders.amount"),
        F.col("orders.store_id"),
        F.col("payments.status"),
        F.col("payments.paid_at"),
        (F.datediff(F.current_date(), F.col("orders.order_date")) / 7).alias("weeks_since_order_date"),
    )
)
