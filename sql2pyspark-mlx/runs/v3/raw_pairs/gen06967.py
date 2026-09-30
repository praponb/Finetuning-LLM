from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("oi.order_id"), "inner")
    .groupBy(F.col("o.channel"))
    .agg(F.min(F.col("oi.quantity")).alias("min_quantity"))
    .orderBy(F.col("channel").desc())
)
