from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_shipments.filter(F.col("shipped_date") <= "2024-07-01").alias("s"), F.col("o.id") == F.col("s.order_id"), "left_anti")
    .select(F.col("o.id"), F.col("o.store_id"), F.col("o.status"), F.col("o.discount"))
)
