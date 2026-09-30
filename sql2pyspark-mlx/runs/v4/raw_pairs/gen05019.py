from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "left")
    .groupBy(F.col("o.channel"))
    .agg(F.count(F.when(F.col("s.status") == "DELIVERED", 1)).alias("delivered_count"))
    .orderBy(F.col("channel").desc())
)
