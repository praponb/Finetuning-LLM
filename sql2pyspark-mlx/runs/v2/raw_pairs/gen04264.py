from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("p.order_id"), "inner")
    .filter(F.col("o.store_id") == 28)
    .select(
        F.col("p.id"),
        F.col("p.method"),
        F.col("p.amount"),
        F.col("p.paid_at"),
        F.col("o.discount"),
        F.col("o.store_id"),
        F.col("o.channel"),
    )
    .orderBy("amount")
)
