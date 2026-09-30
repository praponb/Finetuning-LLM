from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("s.order_id"), "left")
    .filter((F.col("o.shipping_fee") <= 11) & F.col("s.order_id").isin([11, 36, 37]))
    .select(F.col("s.order_id"), F.col("o.channel"))
)
