from pyspark.sql import functions as F

result = (
    df_suppliers.alias("s")
    .join(df_products.alias("p"), F.col("s.id") == F.col("p.supplier_id"), "inner")
    .groupBy(F.col("s.id"))
    .agg(
        F.count(F.col("p.id")).alias("product_count"),
        F.round(((100 * F.sum(F.when(F.col("p.category") == "Home", 1).otherwise(0))) / F.count("*")), 1).alias("pct_home"),
    )
    .orderBy(F.col("product_count").desc())
)
