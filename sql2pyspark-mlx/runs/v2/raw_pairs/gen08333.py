from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "inner")
    .select(
        F.col("p.method"),
        F.col("p.order_id"),
        F.col("p.status"),
        F.col("o.amount"),
        F.col("o.shipping_fee"),
    )
    .orderBy(F.col("status").desc(), "method")
)
