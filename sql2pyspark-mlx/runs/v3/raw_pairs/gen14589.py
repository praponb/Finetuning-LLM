from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "inner")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("o.store_id"), "inner")
    .filter(F.col("p.method").isin(["bank_transfer", "promptpay"]) | (F.col("s.opened_date") < "2025-02-17"))
    .select(
        F.col("p.id"),
        F.col("p.status"),
        F.col("o.order_date"),
        F.col("o.status").alias("order_status"),
        F.col("s.region"),
        F.col("s.city"),
    )
)
