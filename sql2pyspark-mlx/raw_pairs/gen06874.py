from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("p.order_id"), "inner")
    .select(
        F.col("p.id"),
        F.col("p.order_id"),
        F.col("p.paid_at"),
        F.col("o.status"),
        F.datediff(F.current_date(), F.col("p.paid_at")).alias("days_since_paid_at"),
    )
    .orderBy(F.col("id").desc())
)
