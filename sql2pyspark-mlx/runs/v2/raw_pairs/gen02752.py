from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_order_items.alias("oi"), F.col("o.id") == F.col("oi.order_id"), "left_semi")
    .filter(F.col("o.status") != "PENDING")
    .select(F.col("o.id"), F.col("o.order_date"))
    .orderBy("order_date")
)
