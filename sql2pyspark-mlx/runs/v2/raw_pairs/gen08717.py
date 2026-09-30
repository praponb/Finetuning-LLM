from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "outer")
    .join(df_products.alias("p"), F.col("p.id") == F.col("oi.product_id"), "inner")
    .filter(F.col("o.customer_id").isNotNull())
    .groupBy(F.col("p.brand"))
    .agg(F.avg(F.col("oi.unit_price")).alias("avg_unit_price"))
    .orderBy(F.col("avg_unit_price").desc(), "brand")
    .limit(20)
)
