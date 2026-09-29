from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("oi.product_id") == F.col("p.id"), "inner")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "inner")
    .filter(F.col("o.channel").isNotNull())
    .groupBy(F.col("o.status"), F.col("o.channel"))
    .agg(F.round(F.avg(F.col("oi.unit_price")), 2).alias("avg_unit_price"))
    .orderBy("status", "avg_unit_price", "channel")
    .limit(10)
)
