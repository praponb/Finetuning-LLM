from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_shipments.alias("s"), F.col("o.id") == F.col("s.order_id"), "inner")
    .filter(F.col("o.status") != "RETURNED")
    .select(
        F.col("o.shipping_fee"),
        F.col("o.channel"),
        F.col("o.customer_id"),
        F.col("s.cost"),
        F.col("s.order_id"),
        F.col("s.status"),
    )
)
