from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_shipments.alias("s"), F.col("o.id") == F.col("s.order_id"), "left")
    .select(F.col("o.shipping_fee"), F.col("s.delivered_date"), F.col("s.shipped_date"))
)
