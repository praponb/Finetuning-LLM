from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "left")
    .filter(F.col("s.cost") < 73)
    .select(
        F.col("s.shipped_date"),
        F.col("s.delivered_date"),
        F.col("s.order_id"),
        F.col("o.shipping_fee"),
        F.col("o.amount"),
        F.col("o.discount"),
    )
)
