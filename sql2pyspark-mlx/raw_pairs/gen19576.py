from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "outer")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("o.store_id"), "left")
    .select(
        F.col("p.amount"),
        F.col("p.method"),
        F.col("o.channel"),
        F.col("o.status"),
        F.col("s.store_name"),
        F.col("s.city"),
        F.col("o.shipping_fee").cast("int").alias("shipping_fee_int"),
    )
)
