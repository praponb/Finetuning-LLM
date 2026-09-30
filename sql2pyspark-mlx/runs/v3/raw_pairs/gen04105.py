from pyspark.sql import functions as F

supplier_summary = (
    df_products
    .filter(F.col("category") != "Home")
    .groupBy("supplier_id")
    .agg(
        (F.max("price") - F.min("price")).alias("price_range"),
        F.sum(F.when(F.col("category") == "Sports", 1).otherwise(0)).alias("sports_count"),
    )
)

result = (
    df_suppliers.alias("s")
    .join(supplier_summary.alias("ss"), F.col("s.id") == F.col("ss.supplier_id"), "inner")
    .filter(F.col("ss.price_range") > 79)
    .select(
        F.col("s.id"),
        F.col("s.rating"),
        F.col("s.country"),
        F.col("ss.price_range"),
        F.col("ss.sports_count"),
    )
    .orderBy(F.col("price_range").desc(), "id")
    .limit(10)
)
