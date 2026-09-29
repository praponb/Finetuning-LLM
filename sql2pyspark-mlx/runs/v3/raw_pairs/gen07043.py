from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_payments.alias("p"), F.col("p.order_id") == F.col("o.id"), "left")
    .select(
        F.col("o.channel"),
        F.col("o.order_date"),
        F.col("o.discount"),
        F.col("p.amount"),
        F.col("p.paid_at"),
        F.col("p.method"),
    )
)
