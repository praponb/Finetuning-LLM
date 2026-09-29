from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("s.order_id"), "inner")
    .filter(~F.col("s.status").isin(["DELIVERED", "LOST", "IN_TRANSIT"]))
    .groupBy(F.col("o.status"), F.col("o.channel"))
    .agg(F.count(F.col("s.cost")).alias("cost_count"))
    .orderBy("status", "cost_count", "channel")
    .limit(3)
)
