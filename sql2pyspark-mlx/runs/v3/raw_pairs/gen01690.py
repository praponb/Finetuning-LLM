from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.filter(F.col("amount") >= 1350).alias("o"), F.col("oi.order_id") == F.col("o.id"), "left_anti")
    .filter(F.col("oi.unit_price") >= 390)
    .select(F.col("oi.id"), F.col("oi.order_id"))
)
