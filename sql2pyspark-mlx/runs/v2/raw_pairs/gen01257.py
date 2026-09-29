from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_order_items.alias("oi"), F.col("o.id") == F.col("oi.order_id"), "left_semi")
    .select(F.col("o.id"), F.col("o.order_date"), F.col("o.customer_id"))
    .orderBy(F.col("order_date").desc(), "id")
    .limit(10)
)
