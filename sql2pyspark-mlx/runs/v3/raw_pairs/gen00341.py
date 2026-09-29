from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("oi.product_id") == F.col("p.id"), "inner")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "inner")
    .filter(F.col("o.status") == "CANCELLED")
    .groupBy(F.col("p.category"))
    .agg(F.round(F.stddev(F.col("oi.quantity")), 2).alias("stddev_quantity"))
    .orderBy(F.col("stddev_quantity").desc())
)
