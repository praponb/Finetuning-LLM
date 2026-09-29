from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.filter(F.col("store_id") == 6).alias("o"), F.col("s.order_id") == F.col("o.id"), "left_semi")
    .select(F.col("s.id"), F.col("s.order_id"), F.col("s.cost"))
)
