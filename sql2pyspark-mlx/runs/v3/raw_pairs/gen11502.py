from pyspark.sql import functions as F

result = (
    df_payments.alias("payments")
    .join(df_orders.alias("orders"), F.col("payments.order_id") == F.col("orders.id"), "left")
    .select(
        F.col("payments.id"),
        F.col("payments.amount"),
        F.col("orders.discount"),
        F.col("orders.order_date"),
    )
    .orderBy("id", "amount")
)
