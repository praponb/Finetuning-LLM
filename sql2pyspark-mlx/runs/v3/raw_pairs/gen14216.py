from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_payments.alias("p"), F.col("o.id") == F.col("p.order_id"), "left")
    .select(
        F.col("o.store_id"),
        F.col("o.customer_id"),
        F.col("o.discount"),
        F.col("p.amount"),
        F.col("p.order_id"),
        F.col("p.paid_at"),
    )
)
