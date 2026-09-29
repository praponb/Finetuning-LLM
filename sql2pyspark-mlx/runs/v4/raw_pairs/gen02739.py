from pyspark.sql import functions as F

result = (
    df_payments.alias("payments")
    .join(df_orders.alias("orders"), F.col("payments.order_id") == F.col("orders.id"), "left")
    .groupBy(F.col("orders.status"))
    .agg(F.count(F.col("payments.amount")).alias("amount_count"))
    .orderBy("status", "amount_count")
)
