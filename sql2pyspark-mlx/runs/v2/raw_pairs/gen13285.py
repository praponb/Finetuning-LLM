from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("oi.product_id") == F.col("p.id"), "inner")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "right")
    .groupBy(
        F.when(F.col("s.rating") >= 4.4, "high").when(F.col("s.rating") >= 2.3, "mid").otherwise("low").alias("rating_bucket"),
        F.col("s.city"),
    )
    .agg(F.avg(F.col("oi.unit_price")).alias("avg_unit_price"))
)
