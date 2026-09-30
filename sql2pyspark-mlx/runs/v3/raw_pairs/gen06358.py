from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.filter(F.col("customer_id").isin([5, 9, 28, 33])).alias("o"), F.col("oi.order_id") == F.col("o.id"), "left_semi")
    .select(F.col("oi.id"), F.col("oi.quantity"), F.col("oi.unit_price"))
    .orderBy(F.col("unit_price").desc())
)
