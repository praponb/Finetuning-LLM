from pyspark.sql import functions as F

result = (
    df_payments.alias("payments")
    .join(df_orders.alias("orders"), F.col("payments.order_id") == F.col("orders.id"), "left")
    .filter(F.col("orders.status") != "CANCELLED")
    .select(
        F.col("payments.amount"),
        F.col("payments.method"),
        F.col("orders.amount").alias("order_amount"),
    )
)
