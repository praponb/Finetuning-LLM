from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("s.order_id"), "left")
    .select(
        F.col("s.id"),
        F.col("s.delivered_date"),
        F.col("s.shipped_date"),
        F.col("o.shipping_fee"),
        F.col("o.status"),
    )
    .orderBy("status", F.col("id").desc())
    .limit(1)
)
