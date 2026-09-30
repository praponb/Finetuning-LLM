from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "outer")
    .join(df_products.alias("p"), F.col("p.id") == F.col("oi.product_id"), "inner")
    .filter(F.col("oi.unit_price") < 360)
    .groupBy(F.col("o.channel"), F.year(F.col("o.order_date")).alias("order_year"))
    .agg(F.avg(F.col("oi.quantity")).alias("avg_quantity"))
    .filter(F.col("avg_quantity") > 4.8)
    .orderBy(F.col("order_year").desc(), "channel")
    .limit(5)
)
