from pyspark.sql import functions as F

result = (
    df_orders.alias("orders")
    .join(df_payments.filter(F.col("paid_at") >= "2024-12-01").alias("payments"), F.col("orders.id") == F.col("payments.order_id"), "left_anti")
    .select(F.col("orders.id"), F.col("orders.discount"))
)
