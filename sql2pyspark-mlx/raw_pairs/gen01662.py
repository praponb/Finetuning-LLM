from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("s.order_id"), "inner")
    .select(
        F.col("s.id"),
        F.col("s.status"),
        F.col("s.cost"),
        F.col("o.store_id"),
        F.col("o.shipping_fee"),
    )
)
