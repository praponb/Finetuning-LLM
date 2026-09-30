from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "inner")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "outer")
    .filter(F.col("o.channel").isin(["partner", "mobile"]))
    .groupBy(F.col("o.status"))
    .agg(F.sum(F.col("s.cost")).alias("total_cost"))
    .orderBy(F.col("total_cost").desc(), "status")
    .limit(5)
)
