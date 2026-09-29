from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_payments.alias("p"), F.col("p.order_id") == F.col("o.id"), "left")
    .filter(F.col("o.amount") >= 810)
    .select(
        F.col("o.store_id"),
        F.col("o.channel"),
        F.col("o.shipping_fee"),
        F.col("p.amount"),
        F.col("p.paid_at"),
    )
)
