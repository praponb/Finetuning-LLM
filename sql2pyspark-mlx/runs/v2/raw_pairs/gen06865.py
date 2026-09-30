from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "left")
    .groupBy(F.col("o.store_id"))
    .agg((F.max(F.col("s.cost")) - F.min(F.col("s.cost"))).alias("cost_range"))
)
