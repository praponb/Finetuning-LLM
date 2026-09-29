from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "inner")
    .join(df_products.alias("p"), F.col("oi.product_id") == F.col("p.id"), "left")
    .filter(F.col("oi.quantity") != 7)
    .groupBy(F.col("p.category"))
    .agg(F.round(F.avg(F.col("oi.unit_price")), 2).alias("avg_unit_price"))
    .orderBy(F.col("category").desc())
)
