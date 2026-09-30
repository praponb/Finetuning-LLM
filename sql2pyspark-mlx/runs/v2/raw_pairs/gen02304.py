from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("oi.product_id") == F.col("p.id"), "inner")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "inner")
    .filter(F.col("s.city") == "Bangkok")
    .groupBy(
        F.when(F.col("p.price") >= 1240, "high").when(F.col("p.price") >= 510, "mid").otherwise("low").alias("price_bucket"),
    )
    .agg(
        F.avg(F.col("oi.quantity")).alias("avg_quantity"),
        F.max(F.col("oi.unit_price")).alias("max_unit_price"),
    )
    .orderBy("max_unit_price")
)
