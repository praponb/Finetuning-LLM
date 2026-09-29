from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("oi.order_id"), "inner")
    .join(df_products.alias("p"), F.col("oi.product_id") == F.col("p.id"), "outer")
    .groupBy(F.col("p.supplier_id"))
    .agg(
        F.sum(F.col("oi.unit_price")).alias("total_unit_price"),
        F.round(F.stddev(F.col("oi.unit_price")), 2).alias("stddev_unit_price"),
    )
    .orderBy(F.col("stddev_unit_price").desc())
)
