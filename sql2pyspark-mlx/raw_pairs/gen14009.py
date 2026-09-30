from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("oi.product_id") == F.col("p.id"), "left")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "left")
    .filter(F.col("o.amount") >= 1050)
    .groupBy(F.col("o.channel"))
    .agg(F.sum(F.col("oi.unit_price")).alias("total_unit_price"))
    .filter(F.col("total_unit_price") >= 1400)
    .orderBy(F.col("total_unit_price").desc(), "channel")
    .limit(1)
)
