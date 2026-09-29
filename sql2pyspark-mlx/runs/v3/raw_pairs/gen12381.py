from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "outer")
    .filter(F.col("o.status").isin(["CANCELLED", "PENDING"]))
    .select(
        F.col("p.method"),
        F.col("p.order_id"),
        F.col("p.paid_at"),
        F.col("o.discount"),
    )
)
