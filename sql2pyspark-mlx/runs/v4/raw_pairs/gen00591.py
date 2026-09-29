from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("oi.product_id") == F.col("p.id"), "inner")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "inner")
    .groupBy(
        F.when(F.col("s.city").isin(["Chiang Mai", "Singapore"]), "core").otherwise("other").alias("city_group"),
    )
    .agg(
        F.avg(F.col("oi.quantity")).alias("avg_quantity"),
        F.min(F.col("oi.unit_price")).alias("min_unit_price"),
    )
    .orderBy("min_unit_price")
)
