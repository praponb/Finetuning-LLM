from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("s.order_id"), "inner")
    .filter(F.col("o.store_id").isNull())
    .groupBy(F.col("o.status"))
    .agg(F.round(F.stddev(F.col("s.cost")), 2).alias("stddev_cost"))
    .filter(F.col("stddev_cost") > 78)
    .orderBy(F.col("status").desc(), F.col("stddev_cost").desc())
)
