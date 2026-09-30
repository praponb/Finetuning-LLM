from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_shipments.alias("s"), F.col("o.id") == F.col("s.order_id"), "left_semi")
    .filter(F.col("o.status") == "SHIPPED")
    .select(F.col("o.id"), F.col("o.store_id"))
)
