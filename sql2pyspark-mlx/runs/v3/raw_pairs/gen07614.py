from pyspark.sql import functions as F

result = (
    df_orders.alias("orders")
    .join(df_payments.alias("payments"), F.col("orders.id") == F.col("payments.order_id"), "left_semi")
    .select(
        F.col("orders.id"),
        F.col("orders.channel"),
        F.col("orders.order_date"),
        F.col("orders.amount"),
    )
)
