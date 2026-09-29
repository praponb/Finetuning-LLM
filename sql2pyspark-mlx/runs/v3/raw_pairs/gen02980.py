from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "right")
    .select(
        F.col("p.paid_at"),
        F.col("p.amount"),
        F.col("o.amount").alias("order_amount"),
        F.col("o.store_id"),
        F.col("o.channel"),
        (F.col("o.amount") * F.col("o.discount")).alias("amount_times_discount"),
    )
    .orderBy("amount")
)
