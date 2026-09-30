from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "inner")
    .filter(F.col("o.discount") < 0.33)
    .select(
        F.col("p.id"),
        F.col("p.paid_at"),
        F.col("p.amount"),
        F.col("p.status"),
        F.col("o.status").alias("order_status"),
        F.col("o.order_date"),
    )
)
