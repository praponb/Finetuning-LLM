from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "inner")
    .groupBy(F.col("o.status"))
    .agg(F.round(F.avg(F.col("s.cost")), 2).alias("avg_cost"))
    .orderBy(F.col("avg_cost").desc(), "status")
    .limit(1)
)
