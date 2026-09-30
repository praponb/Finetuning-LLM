from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "inner")
    .filter(F.col("p.order_id").isin([3, 6, 25]) & F.col("p.order_id").isNotNull())
    .select(
        F.col("p.id"),
        F.col("p.paid_at"),
        F.col("o.channel"),
        F.col("o.shipping_fee"),
        F.upper(F.col("p.method")).alias("method_upper"),
    )
)
