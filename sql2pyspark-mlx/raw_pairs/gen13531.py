from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.filter(F.col("channel") != "web").alias("o"), F.col("s.order_id") == F.col("o.id"), "left_semi")
    .filter(F.col("s.order_id").isNull())
    .select(F.col("s.id"), F.col("s.cost"), F.col("s.delivered_date"))
)
