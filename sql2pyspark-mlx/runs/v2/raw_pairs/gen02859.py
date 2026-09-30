from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "inner")
    .filter(F.col("p.status").isin(["SUCCESS", "REFUNDED", "FAILED"]) & F.col("o.store_id").isin([27, 28, 37, 39]))
    .select(F.col("p.id"), F.col("p.amount"), F.col("p.paid_at"), F.col("o.store_id"))
    .orderBy("id")
)
