from pyspark.sql import functions as F

result = (
    df_payments.alias("payments")
    .join(df_orders.alias("orders"), F.col("payments.order_id") == F.col("orders.id"), "left")
    .join(df_stores.alias("stores"), F.col("orders.store_id") == F.col("stores.id"), "inner")
    .select(
        F.col("payments.paid_at"),
        F.col("payments.method"),
        F.col("orders.order_date"),
        F.col("stores.opened_date"),
        F.col("stores.city"),
        F.datediff(F.current_date(), F.col("orders.order_date")).alias("days_since_order_date"),
    )
    .orderBy(F.col("days_since_order_date").desc())
)
