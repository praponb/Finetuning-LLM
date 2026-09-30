from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("p.order_id"), "inner")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "left")
    .filter(F.col("o.amount").isNotNull())
    .select(
        F.col("p.amount"),
        F.col("p.paid_at"),
        F.col("o.store_id"),
        F.col("s.region"),
    )
)
