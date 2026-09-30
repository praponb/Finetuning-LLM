from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("s.order_id"), "right")
    .filter(F.col("s.order_id").isNull())
    .select(
        F.col("s.id"),
        F.col("s.cost"),
        F.col("o.discount"),
        F.col("o.channel"),
        F.col("o.shipping_fee"),
    )
    .orderBy(F.col("shipping_fee").desc())
)
