from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "right")
    .filter(F.col("s.status") == "IN_TRANSIT")
    .groupBy(F.col("o.customer_id"))
    .agg(
        F.min(F.col("s.cost")).alias("min_cost"),
        F.round(F.stddev(F.col("s.cost")), 2).alias("stddev_cost"),
    )
    .orderBy(F.col("min_cost").desc())
)
