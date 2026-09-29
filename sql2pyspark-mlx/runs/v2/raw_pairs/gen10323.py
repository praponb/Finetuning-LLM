from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "outer")
    .select(
        F.col("s.carrier"),
        F.col("s.cost"),
        F.col("s.status"),
        F.col("o.shipping_fee"),
    )
)
