from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("oi.product_id") == F.col("p.id"), "inner")
    .join(df_suppliers.alias("s"), F.col("s.id") == F.col("p.supplier_id"), "inner")
    .groupBy(
        F.when(F.col("s.rating") >= 3.6, "high").when(F.col("s.rating") >= 2.5, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(
        F.round(F.stddev(F.col("oi.quantity")), 2).alias("stddev_quantity"),
        F.avg(F.col("oi.unit_price")).alias("avg_unit_price"),
    )
    .filter(F.col("avg_unit_price") > 330)
)
