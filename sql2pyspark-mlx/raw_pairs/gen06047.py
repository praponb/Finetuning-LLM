from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "left")
    .filter(F.col("o.customer_id").isNotNull())
    .groupBy(F.month(F.col("o.order_date")).alias("order_month"), F.col("o.status"))
    .agg((F.max(F.col("s.cost")) - F.min(F.col("s.cost"))).alias("cost_range"))
)
