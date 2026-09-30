from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "outer")
    .filter(F.col("oi.quantity") >= 5)
    .groupBy(F.col("o.channel"))
    .agg(F.sum(F.col("oi.quantity")).alias("total_quantity"))
    .filter(F.col("total_quantity") >= 38)
    .orderBy("channel")
    .limit(3)
)
