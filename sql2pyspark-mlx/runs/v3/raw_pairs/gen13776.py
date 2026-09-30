from pyspark.sql import functions as F

result = (
    df_orders.alias("orders")
    .join(df_payments.alias("payments"), F.col("payments.order_id") == F.col("orders.id"), "left")
    .select(
        F.col("orders.status"),
        F.col("payments.order_id"),
        F.col("payments.method"),
        F.coalesce(F.col("orders.channel"), F.lit("unassigned")).alias("channel_filled"),
    )
)
