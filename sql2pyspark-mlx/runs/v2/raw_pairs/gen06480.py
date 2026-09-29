from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("oi.product_id") == F.col("p.id"), "inner")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "inner")
    .groupBy(F.col("p.brand"), F.col("p.category"))
    .agg(
        F.min(F.col("oi.unit_price")).alias("min_unit_price"),
        F.round(F.avg(F.col("oi.unit_price")), 2).alias("avg_unit_price"),
    )
    .orderBy(F.col("avg_unit_price").desc())
)
