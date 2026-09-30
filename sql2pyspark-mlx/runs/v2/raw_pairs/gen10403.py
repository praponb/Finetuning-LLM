from pyspark.sql import functions as F

result = (
    df_orders.alias("orders")
    .join(df_payments.filter(F.hour("paid_at").between(12, 15)).alias("payments"), F.col("orders.id") == F.col("payments.order_id"), "left_anti")
    .select(F.col("orders.id"), F.col("orders.discount"), F.col("orders.status"))
)
