from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "inner")
    .filter(F.col("o.shipping_fee").isNotNull())
    .groupBy(F.col("o.status"))
    .agg(F.max(F.col("oi.unit_price")).alias("max_unit_price"))
    .orderBy("status", "max_unit_price")
    .limit(20)
)
