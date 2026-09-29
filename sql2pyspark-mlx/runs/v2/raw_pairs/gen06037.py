from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_order_items.filter(F.col("order_id") == 29).alias("oi"), F.col("o.id") == F.col("oi.order_id"), "left_anti")
    .select(F.col("o.id"), F.col("o.discount"))
    .orderBy(F.col("id").desc(), F.col("discount").desc())
)
