from pyspark.sql import functions as F

result = (
    df_orders.alias("orders")
    .join(df_payments.alias("payments"), F.col("payments.order_id") == F.col("orders.id"), "left")
    .groupBy(F.col("orders.channel"))
    .agg(F.count(F.col("payments.id")).alias("payment_count"))
)
