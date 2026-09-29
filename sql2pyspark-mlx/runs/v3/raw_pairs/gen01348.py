from pyspark.sql import functions as F

supplier_stats = (
    df_products
    .groupBy("supplier_id")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("category") == "Books", 1).otherwise(0))) / F.count("*")), 1).alias("pct_books"),
    )
)

result = (
    df_suppliers.alias("s")
    .join(supplier_stats.alias("ss"), F.col("s.id") == F.col("ss.supplier_id"), "inner")
    .filter(F.col("ss.pct_books") >= 62)
    .select(F.col("s.rating"), F.col("s.supplier_name"), F.col("ss.pct_books"))
    .orderBy(F.col("rating").desc())
)
