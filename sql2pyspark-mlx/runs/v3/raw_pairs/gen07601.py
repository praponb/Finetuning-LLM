from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "inner")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "inner")
    .filter(F.col("p.method").isin(["bank_transfer", "cash", "promptpay"]))
    .select(
        F.col("p.id"),
        F.col("p.method"),
        F.col("p.paid_at"),
        F.col("o.status"),
        F.col("s.opened_date"),
        F.col("s.store_name"),
    )
)
