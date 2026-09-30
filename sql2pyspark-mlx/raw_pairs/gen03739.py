from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "inner")
    .filter(F.col("o.customer_id").isin([26, 29]))
    .select(
        F.col("p.id"),
        F.col("p.paid_at"),
        F.col("p.amount"),
        F.col("p.method"),
        F.col("o.store_id"),
        F.datediff(F.current_date(), F.col("p.paid_at")).alias("days_since_paid_at"),
    )
    .orderBy("days_since_paid_at", "id")
    .limit(50)
)
