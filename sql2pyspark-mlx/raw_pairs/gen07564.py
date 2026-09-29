from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_shipments.filter(F.col("status") == "IN_TRANSIT").alias("s"), F.col("o.id") == F.col("s.order_id"), "left_anti")
    .select(F.col("o.id"), F.col("o.amount"))
)
