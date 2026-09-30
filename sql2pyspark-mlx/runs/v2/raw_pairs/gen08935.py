from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "left")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "left")
    .filter(F.col("c.age") <= 56)
    .groupBy(F.col("o.status"), F.col("o.customer_id"))
    .agg(
        F.count("*").alias("cnt"),
        F.count(F.col("s.cost")).alias("cost_count"),
    )
    .filter(F.col("cnt") >= 5)
)
