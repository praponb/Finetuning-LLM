from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_payments.filter(F.col("order_id").isin([11, 17, 32, 38])).alias("p"), F.col("o.id") == F.col("p.order_id"), "left_anti")
    .filter(F.col("o.discount") <= 0.16)
    .select(
        F.col("o.id"),
        F.col("o.status"),
        F.col("o.channel"),
        F.col("o.shipping_fee"),
    )
)
