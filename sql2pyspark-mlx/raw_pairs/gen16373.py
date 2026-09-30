from pyspark.sql import functions as F

result = (
    df_payments.alias("payments")
    .join(df_orders.alias("orders"), F.col("orders.id") == F.col("payments.order_id"), "left")
    .filter((F.col("orders.discount") > 0.36) & (F.col("orders.store_id") == 12))
    .select(
        F.col("payments.paid_at"),
        F.col("orders.order_date"),
        F.col("orders.channel"),
        F.col("orders.shipping_fee"),
    )
    .orderBy(F.col("paid_at").desc())
)
