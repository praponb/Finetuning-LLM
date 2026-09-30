from pyspark.sql import functions as F

supplier_summary = (
    df_products
    .filter(F.col("category").isin(["Electronics", "Home", "Books"]))
    .groupBy("supplier_id")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("brand") == "Acme", 1).otherwise(0))) / F.count("*")), 1).alias("pct_acme"),
    )
)

result = (
    df_suppliers.alias("s")
    .join(supplier_summary.alias("ss"), F.col("s.id") == F.col("ss.supplier_id"), "inner")
    .filter(F.col("ss.pct_acme") >= 51)
    .select(
        F.col("s.id"),
        F.col("s.supplier_name"),
        F.col("s.city"),
        F.col("ss.pct_acme"),
    )
    .orderBy(F.col("supplier_name").desc(), "city", "id")
    .limit(5)
)
