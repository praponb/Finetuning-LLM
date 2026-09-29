from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("s.order_id"), "inner")
    .groupBy(F.col("o.channel"))
    .agg(F.round(F.avg(F.col("s.cost")), 2).alias("avg_cost"))
    .orderBy(F.col("avg_cost").desc(), "channel")
)
