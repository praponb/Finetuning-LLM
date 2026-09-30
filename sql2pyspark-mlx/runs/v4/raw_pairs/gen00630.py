from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "right")
    .filter(F.col("s.order_id").isin([28, 34]))
    .select(F.col("s.order_id"), F.col("o.shipping_fee"), F.col("o.amount"))
    .orderBy(F.col("order_id").desc())
)
