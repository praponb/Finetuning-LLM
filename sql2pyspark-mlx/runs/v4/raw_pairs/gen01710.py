from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_payments.alias("p"), F.col("p.order_id") == F.col("o.id"), "inner")
    .filter(F.col("o.channel") != "partner")
    .select(
        F.col("o.shipping_fee"),
        F.col("o.store_id"),
        F.col("p.paid_at"),
        F.col("p.amount"),
        F.col("p.status"),
    )
    .orderBy(F.col("status").desc())
)
