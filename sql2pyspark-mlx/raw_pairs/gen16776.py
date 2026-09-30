from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "inner")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "inner")
    .filter(F.col("o.store_id").isin([3, 8, 11, 21]))
    .groupBy(F.col("c.segment"))
    .agg(
        F.round(F.stddev(F.col("s.cost")), 2).alias("stddev_cost"),
        F.min(F.col("s.cost")).alias("min_cost"),
    )
    .orderBy(F.col("stddev_cost").desc(), "segment")
    .limit(5)
)
