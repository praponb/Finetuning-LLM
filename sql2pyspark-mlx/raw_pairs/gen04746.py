from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("s.order_id"), "inner")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("o.customer_id"), "left")
    .filter(F.col("c.credit_limit") >= 9100)
    .groupBy(F.col("c.country"))
    .agg(F.sum(F.col("s.cost")).alias("total_cost"))
    .orderBy(F.col("total_cost").desc(), F.col("country").desc())
)
