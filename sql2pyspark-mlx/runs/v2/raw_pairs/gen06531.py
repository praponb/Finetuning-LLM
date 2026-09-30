from pyspark.sql import functions as F

result = (
    df_payments.alias("payments")
    .join(df_orders.alias("orders"), F.col("orders.id") == F.col("payments.order_id"), "outer")
    .select(
        F.col("payments.id"),
        F.col("payments.method"),
        F.col("payments.order_id"),
        F.col("payments.amount"),
        F.col("orders.channel"),
    )
    .orderBy(F.col("order_id").desc())
)
