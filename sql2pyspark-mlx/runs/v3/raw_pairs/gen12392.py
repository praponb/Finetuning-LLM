from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "left")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "right")
    .filter(F.col("o.order_date").between("2024-01-22", "2024-06-01"))
    .groupBy(F.col("o.channel"))
    .agg(F.min(F.col("s.cost")).alias("min_cost"))
    .orderBy(F.col("channel").desc())
)
