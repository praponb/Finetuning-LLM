from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "inner")
    .filter((F.col("s.shipped_date") <= "2024-10-01") & (F.col("o.shipping_fee") > 11))
    .select(
        F.col("s.delivered_date"),
        F.col("o.channel"),
        F.col("o.discount"),
        F.col("o.shipping_fee"),
    )
)
