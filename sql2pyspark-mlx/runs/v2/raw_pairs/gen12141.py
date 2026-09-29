from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_shipments.alias("s"), F.col("o.id") == F.col("s.order_id"), "left")
    .filter((F.col("s.delivered_date") <= "2025-10-11") | (F.col("o.discount") >= 0.31))
    .select(F.col("o.shipping_fee"), F.col("s.delivered_date"))
    .orderBy("delivered_date")
)
