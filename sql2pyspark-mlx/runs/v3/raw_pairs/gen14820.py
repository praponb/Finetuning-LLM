from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "inner")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "inner")
    .select(
        F.col("p.paid_at"),
        F.col("o.discount"),
        F.col("o.channel"),
        F.col("s.city"),
        F.col("s.store_name"),
    )
)
