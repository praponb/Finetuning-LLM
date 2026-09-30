from pyspark.sql import functions as F

result = (
    df_orders.alias("orders")
    .join(df_payments.filter(F.col("amount").isNotNull()).alias("payments"), F.col("orders.id") == F.col("payments.order_id"), "left_semi")
    .filter(F.col("orders.customer_id").isNotNull())
    .select(
        F.col("orders.id"),
        F.col("orders.store_id"),
        F.col("orders.channel"),
        F.col("orders.order_date"),
    )
)
