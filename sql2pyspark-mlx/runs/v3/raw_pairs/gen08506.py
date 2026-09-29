from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("oi.product_id") == F.col("p.id"), "inner")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "inner")
    .filter(F.col("s.rating") < 3.7)
    .groupBy(F.col("s.country"))
    .agg(
        F.count("*").alias("cnt"),
        F.min(F.col("oi.quantity")).alias("min_quantity"),
    )
    .orderBy(F.col("min_quantity").desc(), "country")
)
