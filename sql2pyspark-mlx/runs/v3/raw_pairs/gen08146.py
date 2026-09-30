from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_order_items.filter(F.col("unit_price") <= 140).alias("oi"), F.col("o.id") == F.col("oi.order_id"), "left_anti")
    .filter(F.col("o.amount") < 1210)
    .select(F.col("o.id"), F.col("o.status"), F.col("o.store_id"), F.col("o.amount"))
    .orderBy(F.col("id").desc())
)
