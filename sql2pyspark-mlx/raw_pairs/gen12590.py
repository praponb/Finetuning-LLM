from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "left")
    .filter(F.col("p.status").isin(["REFUNDED", "FAILED"]))
    .select(
        F.col("p.id"),
        F.col("p.paid_at"),
        F.col("o.customer_id"),
        F.col("o.store_id"),
        F.col("o.shipping_fee"),
        F.col("p.paid_at").cast("date").alias("paid_at_day"),
    )
    .orderBy(F.col("store_id").desc())
)
