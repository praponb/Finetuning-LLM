from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "left")
    .select(
        F.col("p.id"),
        F.col("p.paid_at"),
        F.col("p.amount"),
        F.col("p.order_id"),
        F.col("o.discount"),
        F.col("o.channel"),
    )
    .orderBy(F.col("discount").desc())
)
