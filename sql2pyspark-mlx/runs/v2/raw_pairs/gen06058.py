from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "inner")
    .select(
        F.col("s.id"),
        F.col("s.delivered_date"),
        F.col("s.status"),
        F.col("o.shipping_fee"),
        F.col("o.order_date"),
        F.col("o.amount"),
        (F.col("o.discount") + F.col("o.shipping_fee")).alias("discount_plus_shipping_fee"),
    )
)
