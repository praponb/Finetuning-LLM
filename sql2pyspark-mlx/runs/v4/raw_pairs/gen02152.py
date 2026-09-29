from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("oi.order_id"), "left")
    .filter(F.col("o.order_date") <= "2023-08-21")
    .select(F.col("oi.id"), F.col("oi.quantity"), F.col("o.channel"))
    .orderBy(F.col("quantity").desc(), "id")
    .limit(10)
)
