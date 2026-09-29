from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "inner")
    .filter(F.col("o.order_date") <= "2024-09-01")
    .groupBy(F.col("o.channel"))
    .agg(F.avg(F.col("s.cost")).alias("avg_cost"))
)
