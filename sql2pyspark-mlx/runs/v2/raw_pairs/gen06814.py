from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "inner")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "right")
    .groupBy(F.quarter(F.col("o.order_date")).alias("order_quarter"), F.col("o.status"))
    .agg(F.round(F.stddev(F.col("s.cost")), 2).alias("stddev_cost"))
    .orderBy("status", "order_quarter")
    .limit(50)
)
