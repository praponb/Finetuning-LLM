from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "outer")
    .select(
        F.col("p.paid_at"),
        F.col("p.amount"),
        F.col("p.status"),
        F.col("o.store_id"),
        F.col("o.customer_id"),
        F.col("o.order_date"),
    )
    .orderBy(F.col("amount").desc())
)
