from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("oi.product_id") == F.col("p.id"), "inner")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "left")
    .groupBy(F.col("o.channel"))
    .agg(F.sum(F.col("oi.quantity")).alias("total_quantity"))
    .orderBy(F.col("channel").desc())
    .limit(10)
)
