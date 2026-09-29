from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_payments.alias("p"), F.col("p.order_id") == F.col("o.id"), "left")
    .filter(F.col("p.order_id").isNotNull() | (F.col("o.amount") >= 1480))
    .select(F.col("o.customer_id"), F.col("p.order_id"), F.col("p.paid_at"))
    .orderBy("order_id")
)
