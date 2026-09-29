from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("oi.order_id"), "left")
    .join(df_products.alias("p"), F.col("p.id") == F.col("oi.product_id"), "left")
    .groupBy(F.col("p.brand"))
    .agg(
        F.round(F.stddev(F.col("oi.unit_price")), 2).alias("stddev_unit_price"),
        F.min(F.col("oi.unit_price")).alias("min_unit_price"),
    )
    .orderBy("brand")
    .limit(5)
)
