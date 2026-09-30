from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("s.order_id"), "right")
    .filter(F.col("s.cost") <= 80)
    .groupBy(F.col("o.status"))
    .agg(F.min(F.col("s.delivered_date")).alias("min_delivered_date"))
    .orderBy("min_delivered_date", "status")
    .limit(5)
)
