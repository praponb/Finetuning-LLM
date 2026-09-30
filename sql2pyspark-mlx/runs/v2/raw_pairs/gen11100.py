from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "outer")
    .filter(F.col("oi.order_id") == 7)
    .groupBy(F.date_trunc("MONTH", F.col("o.order_date")).alias("order_month"))
    .agg(F.sum((F.col("oi.unit_price") * F.col("oi.quantity"))).alias("revenue"))
    .orderBy("order_month")
)
