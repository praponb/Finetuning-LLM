from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "outer")
    .join(df_products.alias("p"), F.col("p.id") == F.col("oi.product_id"), "inner")
    .filter(F.col("oi.unit_price") < 360)
    .groupBy(F.col("o.channel"), F.col("o.status"))
    .agg(F.round(F.avg(F.col("oi.unit_price")), 2).alias("avg_unit_price"))
    .filter(F.col("avg_unit_price") > 42)
    .orderBy("status", "channel")
    .limit(5)
)
