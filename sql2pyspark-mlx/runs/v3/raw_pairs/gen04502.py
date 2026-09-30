from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "outer")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "inner")
    .groupBy(F.col("c.country"))
    .agg(F.sum(F.col("s.cost")).alias("total_cost"))
    .filter(F.col("total_cost") >= 410)
    .orderBy(F.col("total_cost").desc())
)
