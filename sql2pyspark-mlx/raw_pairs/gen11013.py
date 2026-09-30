from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "inner")
    .filter(F.col("s.supplier_name").like("Sia%"))
    .groupBy(F.col("s.city"))
    .agg(
        F.avg(F.col("p.price")).alias("avg_price"),
        F.min(F.col("p.price")).alias("min_price"),
    )
    .orderBy(F.col("avg_price").desc(), "city")
    .limit(20)
)
