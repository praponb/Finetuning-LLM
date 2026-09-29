from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("s.order_id"), "left")
    .filter(F.col("o.shipping_fee") <= 17)
    .select(
        F.col("s.id"),
        F.col("s.status"),
        F.col("s.order_id"),
        F.col("o.channel"),
        F.col("o.store_id"),
        F.col("o.shipping_fee"),
        F.coalesce(F.col("s.cost"), F.lit(0)).alias("cost_filled"),
    )
)
