from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_payments.filter(F.col("paid_at") < "2023-12-01").alias("p"), F.col("o.id") == F.col("p.order_id"), "left_semi")
    .select(
        F.col("o.id"),
        F.col("o.customer_id"),
        F.col("o.channel"),
        F.col("o.shipping_fee"),
    )
    .orderBy(F.col("channel").desc())
)
