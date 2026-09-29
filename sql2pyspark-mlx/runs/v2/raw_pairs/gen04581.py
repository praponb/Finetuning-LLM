from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("oi.product_id") == F.col("p.id"), "inner")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("oi.order_id"), "left")
    .filter(F.col("o.discount") <= 0.17)
    .groupBy(F.col("o.channel"))
    .agg(F.avg(F.col("oi.unit_price")).alias("avg_unit_price"))
    .orderBy(F.col("avg_unit_price").desc(), "channel")
)
