from pyspark.sql import functions as F

result = (
    df_payments.alias("payments")
    .join(df_orders.filter(F.col("status") == "RETURNED").alias("orders"), F.col("payments.order_id") == F.col("orders.id"), "left_semi")
    .filter(F.col("payments.paid_at") >= "2024-11-01")
    .select(F.col("payments.id"), F.col("payments.paid_at"))
    .orderBy("paid_at")
)
