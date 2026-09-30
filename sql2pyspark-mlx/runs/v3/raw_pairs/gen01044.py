from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("p.id") == F.col("oi.product_id"), "inner")
    .join(df_suppliers.alias("s"), F.col("s.id") == F.col("p.supplier_id"), "inner")
    .groupBy(F.col("s.city"))
    .agg(
        F.max(F.col("oi.unit_price")).alias("max_unit_price"),
        F.countDistinct(F.col("oi.unit_price")).alias("unique_products"),
    )
    .orderBy("city")
)
