from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "left")
    .select(
        F.col("p.id"),
        F.col("p.amount"),
        F.col("o.channel"),
        F.col("o.status"),
        F.col("o.shipping_fee"),
        F.round(F.col("o.shipping_fee"), 1).alias("shipping_fee_rounded"),
    )
)
