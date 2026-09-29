from pyspark.sql import functions as F

supplier_stats = (
    df_products
    .groupBy("supplier_id")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("category") == "Sports", 1).otherwise(0))) / F.count("*")), 1).alias("pct_sports"),
    )
)

result = (
    df_suppliers.alias("s")
    .join(supplier_stats.alias("S"), F.col("s.id") == F.col("S.supplier_id"), "inner")
    .filter(F.col("S.pct_sports") >= 55)
    .select(F.col("s.id"), F.col("s.supplier_name"), F.col("S.pct_sports"))
    .orderBy("pct_sports", "id")
    .limit(10)
)
