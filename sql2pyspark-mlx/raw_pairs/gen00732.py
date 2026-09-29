from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.filter(F.col("status") == "COMPLETED").alias("o"), F.col("p.order_id") == F.col("o.id"), "left_semi")
    .filter(F.col("p.order_id").isin([23, 27]))
    .select(F.col("p.id"), F.col("p.status"), F.col("p.paid_at"))
    .orderBy("paid_at")
)
