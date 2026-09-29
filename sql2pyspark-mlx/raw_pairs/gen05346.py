from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "right")
    .select(
        F.col("p.id"),
        F.col("p.status"),
        F.col("p.paid_at"),
        F.col("p.amount"),
        F.col("o.store_id"),
        F.col("o.discount"),
        F.col("o.order_date"),
        F.round((F.col("p.amount") * 1.1), 2).alias("amount_scaled"),
    )
    .orderBy("paid_at")
)
