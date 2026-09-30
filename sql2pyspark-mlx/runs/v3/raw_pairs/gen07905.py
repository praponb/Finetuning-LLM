from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_shipments.alias("s"), F.col("o.id") == F.col("s.order_id"), "left")
    .select(F.col("o.amount"), F.col("s.shipped_date"), F.col("s.delivered_date"))
    .orderBy("amount")
)
