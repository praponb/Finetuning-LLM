from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("oi.order_id"), "right")
    .groupBy(F.col("o.status"))
    .agg(F.countDistinct(F.col("oi.quantity")).alias("unique_orders"))
    .filter(F.col("unique_orders") > 1)
    .orderBy(F.col("unique_orders").desc())
)
