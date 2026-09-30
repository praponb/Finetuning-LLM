from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("p.order_id"), "inner")
    .filter(F.col("p.order_id").isNull() & (F.col("p.amount") <= 1570))
    .select(
        F.col("p.amount"),
        F.col("p.paid_at"),
        F.col("p.order_id"),
        F.col("o.status"),
        F.col("o.discount"),
    )
)
