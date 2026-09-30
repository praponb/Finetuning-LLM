from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "right")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "inner")
    .groupBy(F.col("o.status"))
    .agg(F.avg(F.col("s.cost")).alias("avg_cost"))
    .filter(F.col("avg_cost") > 87)
    .orderBy(F.col("status").desc())
)
