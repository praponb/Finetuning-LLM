from pyspark.sql import functions as F

result = (
    df_payments.alias("payments")
    .join(df_orders.filter(F.col("channel") == "mobile").alias("orders"), F.col("payments.order_id") == F.col("orders.id"), "left_semi")
    .filter(F.col("payments.amount").between(470, 900))
    .select(F.col("payments.id"), F.col("payments.amount"))
)
