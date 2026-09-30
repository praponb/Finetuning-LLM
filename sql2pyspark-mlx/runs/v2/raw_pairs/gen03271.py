from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "inner")
    .filter(F.col("o.channel").isin(["mobile", "partner", "web"]))
    .select(
        F.col("s.id"),
        F.col("s.status"),
        F.col("s.carrier"),
        F.col("s.order_id"),
        F.col("o.shipping_fee"),
        F.col("o.store_id"),
        F.col("o.amount"),
    )
)
