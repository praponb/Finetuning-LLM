from pyspark.sql import functions as F

result = (
    df_orders.alias("orders")
    .join(df_payments.filter(F.col("status") == "REFUNDED").alias("payments"), F.col("orders.id") == F.col("payments.order_id"), "left_semi")
    .filter(F.col("orders.amount") > 1450)
    .select(F.col("orders.id"), F.col("orders.channel"), F.col("orders.status"))
)
