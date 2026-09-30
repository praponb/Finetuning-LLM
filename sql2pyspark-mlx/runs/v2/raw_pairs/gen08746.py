from pyspark.sql import functions as F

result = (
    df_payments.alias("payments")
    .join(df_orders.alias("orders"), F.col("payments.order_id") == F.col("orders.id"), "outer")
    .filter(F.col("payments.order_id").isNotNull())
    .select(
        F.col("payments.id"),
        F.col("payments.method"),
        F.col("payments.status"),
        F.col("orders.amount"),
        F.col("orders.discount"),
    )
    .orderBy("status")
)
