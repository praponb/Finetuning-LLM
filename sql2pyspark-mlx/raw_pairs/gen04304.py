from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "inner")
    .join(df_products.alias("p"), F.col("oi.product_id") == F.col("p.id"), "inner")
    .groupBy(F.col("o.store_id"))
    .agg(F.round(F.stddev(F.col("oi.quantity")), 2).alias("stddev_quantity"))
    .orderBy(F.col("store_id").desc())
)
