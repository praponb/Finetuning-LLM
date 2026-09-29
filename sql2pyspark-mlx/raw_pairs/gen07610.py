from pyspark.sql import functions as F

result = (
    df_payments.alias("payments")
    .join(df_orders.filter(F.col("discount") <= 0.4).alias("orders"), F.col("payments.order_id") == F.col("orders.id"), "left_semi")
    .select(F.col("payments.id"), F.col("payments.amount"), F.col("payments.status"))
    .orderBy("amount")
)
