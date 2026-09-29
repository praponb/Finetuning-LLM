from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_payments.alias("p"), F.col("p.order_id") == F.col("o.id"), "inner")
    .filter(F.col("p.paid_at") >= "2024-07-03")
    .select(
        F.col("o.store_id"),
        F.col("p.order_id"),
        F.col("p.paid_at"),
        F.col("p.amount"),
    )
    .orderBy("store_id")
)
