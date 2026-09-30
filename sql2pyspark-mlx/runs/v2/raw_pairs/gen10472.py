from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("p.id") == F.col("oi.product_id"), "inner")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "inner")
    .filter(F.col("o.shipping_fee") <= 12)
    .groupBy(F.col("o.status"))
    .agg(F.sum(F.col("oi.unit_price")).alias("total_unit_price"))
    .filter(F.col("total_unit_price") >= 1700)
    .orderBy("status")
    .limit(1)
)
