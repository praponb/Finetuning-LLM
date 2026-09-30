from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "inner")
    .join(df_products.alias("p"), F.col("oi.product_id") == F.col("p.id"), "right")
    .filter(F.size(F.col("p.tags")) >= 2)
    .groupBy(F.col("o.status"), F.col("o.channel"))
    .agg(F.round(F.stddev(F.col("oi.quantity")), 2).alias("stddev_quantity"))
    .orderBy(F.col("stddev_quantity").desc(), F.col("status").desc())
)
