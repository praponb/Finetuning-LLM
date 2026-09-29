from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "outer")
    .join(df_products.alias("p"), F.col("oi.product_id") == F.col("p.id"), "inner")
    .groupBy(F.col("o.status"), F.col("o.channel"))
    .agg(F.sum(F.col("oi.unit_price")).alias("total_unit_price"))
    .filter(F.col("total_unit_price") > 1500)
    .orderBy("channel", "status")
    .limit(1)
)
