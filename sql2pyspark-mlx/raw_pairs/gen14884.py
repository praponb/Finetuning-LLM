from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_payments.alias("p"), F.col("p.order_id") == F.col("o.id"), "left")
    .filter(F.col("p.method").isNotNull() & F.col("p.status").isin(["FAILED", "REFUNDED"]))
    .select(
        F.col("o.store_id"),
        F.col("o.order_date"),
        F.col("o.shipping_fee"),
        F.col("p.order_id"),
        F.col("p.paid_at"),
    )
)
