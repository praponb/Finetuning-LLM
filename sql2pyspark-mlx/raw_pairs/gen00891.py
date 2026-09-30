from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "inner")
    .groupBy(F.col("s.country"), F.col("s.city"))
    .agg(
        F.max(F.col("p.price")).alias("max_price"),
        F.min(F.col("p.price")).alias("min_price"),
    )
    .filter(F.col("min_price") >= 440)
    .orderBy("city", "country")
    .limit(1)
)
