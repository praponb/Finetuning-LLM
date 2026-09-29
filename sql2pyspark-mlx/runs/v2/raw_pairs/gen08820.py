from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.filter(F.col("amount") <= 1240).alias("o"), F.col("oi.order_id") == F.col("o.id"), "left_anti")
    .select(F.col("oi.id"), F.col("oi.order_id"), F.col("oi.unit_price"))
    .orderBy(F.col("unit_price").desc())
)
