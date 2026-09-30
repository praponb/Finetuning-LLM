from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("p.order_id"), "outer")
    .select(
        F.col("p.id"),
        F.col("p.status"),
        F.col("o.store_id"),
        F.col("o.shipping_fee"),
        F.col("o.channel"),
    )
)
