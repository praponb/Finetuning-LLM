from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "left")
    .filter(~F.col("o.channel").isin(["store", "web", "partner"]))
    .groupBy(F.col("o.status"), F.col("o.channel"))
    .agg(F.avg(F.col("oi.unit_price")).alias("avg_unit_price"))
    .orderBy("status")
)
