from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "outer")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("o.customer_id"), "outer")
    .filter(F.col("c.segment") == "Corporate")
    .groupBy(F.year(F.col("o.order_date")).alias("order_year"))
    .agg(F.round(F.stddev(F.col("s.cost")), 2).alias("stddev_cost"))
    .orderBy(F.col("order_year").desc(), "stddev_cost")
)
