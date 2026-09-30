from pyspark.sql import functions as F

result = (
    df_payments.alias("payments")
    .join(df_orders.alias("orders"), F.col("orders.id") == F.col("payments.order_id"), "left")
    .filter(F.col("orders.store_id").isNotNull())
    .select(F.col("payments.paid_at"), F.col("payments.status"), F.col("orders.amount"))
    .orderBy("paid_at")
)
