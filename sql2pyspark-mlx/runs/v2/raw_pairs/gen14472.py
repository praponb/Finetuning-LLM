from pyspark.sql import functions as F

result = (
    df_orders.alias("orders")
    .join(df_payments.alias("payments"), F.col("payments.order_id") == F.col("orders.id"), "left")
    .filter(F.col("orders.status").isin(["COMPLETED", "SHIPPED"]) & (F.col("payments.method") == "card"))
    .select(
        F.col("orders.store_id"),
        F.col("orders.shipping_fee"),
        F.col("payments.method"),
        F.col("payments.status"),
        F.col("payments.paid_at"),
        F.concat_ws("-", F.col("payments.method"), F.col("payments.status")).alias("method_status_key"),
    )
    .orderBy("paid_at")
)
