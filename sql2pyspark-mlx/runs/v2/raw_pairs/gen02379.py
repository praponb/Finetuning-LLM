from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.filter(F.year("order_date") == 2024).alias("o"), F.col("oi.order_id") == F.col("o.id"), "left_semi")
    .select(F.col("oi.id"), F.col("oi.product_id"), F.col("oi.order_id"))
    .orderBy(F.col("order_id").desc(), "id")
    .limit(1)
)
