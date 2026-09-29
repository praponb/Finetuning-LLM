from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("oi.order_id"), "left")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "right")
    .filter(F.col("oi.unit_price").between(160, 380))
    .groupBy(F.col("o.channel"), F.col("o.status"))
    .agg(F.round(F.avg(F.col("oi.unit_price")), 2).alias("avg_unit_price"))
    .filter(F.col("avg_unit_price") > 31)
)
