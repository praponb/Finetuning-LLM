from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "outer")
    .join(df_products.alias("p"), F.col("oi.product_id") == F.col("p.id"), "inner")
    .filter(F.array_contains(F.col("p.tags"), "clearance"))
    .groupBy(F.col("o.channel"))
    .agg(F.avg(F.col("oi.quantity")).alias("avg_quantity"))
    .orderBy(F.col("channel").desc())
    .limit(3)
)
