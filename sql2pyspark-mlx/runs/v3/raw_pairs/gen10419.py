from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_payments.alias("p"), F.col("p.order_id") == F.col("o.id"), "left")
    .filter(F.hour(F.col("p.paid_at")).between(7, 12))
    .select(F.col("o.status"), F.col("p.amount"), F.col("p.order_id"))
)
