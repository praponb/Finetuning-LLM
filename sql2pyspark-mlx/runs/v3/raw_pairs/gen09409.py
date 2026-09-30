from pyspark.sql import functions as F

result = (
    df_payments.alias("payments")
    .join(df_orders.alias("orders"), F.col("payments.order_id") == F.col("orders.id"), "left")
    .filter(F.col("payments.order_id").isin([2, 8, 30, 36]) | F.col("orders.channel").isNull())
    .select(
        F.col("payments.id"),
        F.col("payments.paid_at"),
        F.col("payments.status"),
        F.col("orders.store_id"),
        F.col("orders.shipping_fee"),
        F.initcap(F.trim(F.col("payments.status"))).alias("status_title"),
    )
    .orderBy(F.col("store_id").desc())
)
