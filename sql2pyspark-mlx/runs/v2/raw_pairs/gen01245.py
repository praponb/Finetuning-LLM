from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("oi.order_id"), "inner")
    .join(df_products.alias("p"), F.col("oi.product_id") == F.col("p.id"), "inner")
    .groupBy(F.col("o.status"))
    .agg(F.avg(F.col("oi.quantity")).alias("avg_quantity"))
    .filter(F.col("avg_quantity") > 4.1)
)
