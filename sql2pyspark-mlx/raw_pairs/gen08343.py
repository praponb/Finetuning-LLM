from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_shipments.alias("s"), F.col("o.id") == F.col("s.order_id"), "inner")
    .filter(F.col("s.cost") < 81)
    .select(
        F.col("o.shipping_fee"),
        F.col("o.discount"),
        F.col("s.shipped_date"),
        F.col("s.order_id"),
    )
    .orderBy(F.col("shipping_fee").desc(), F.col("discount").desc())
)
