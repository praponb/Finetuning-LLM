from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "inner")
    .groupBy(F.col("o.status"))
    .agg(F.countDistinct(F.col("oi.unit_price")).alias("unique_products"))
    .orderBy("unique_products")
)
