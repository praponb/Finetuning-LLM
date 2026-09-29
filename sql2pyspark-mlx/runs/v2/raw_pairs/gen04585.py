from pyspark.sql import functions as F

order_stats = (
    df_shipments
    .groupBy("order_id")
    .agg(F.countDistinct("cost").alias("unique_statuss"))
)

result = (
    df_orders.alias("o")
    .join(order_stats.alias("t"), F.col("o.id") == F.col("t.order_id"), "left")
    .filter(F.col("t.unique_statuss") > 6)
    .select(F.col("o.amount"), F.col("t.unique_statuss"))
)
