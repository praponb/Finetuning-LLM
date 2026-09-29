from pyspark.sql import functions as F

supplier_stats = (
    df_products
    .groupBy("supplier_id")
    .agg(
        F.count(F.when(F.col("category") == "Electronics", 1)).alias("electronics_count"),
    )
)

result = (
    df_suppliers.alias("s")
    .join(supplier_stats.alias("t"), F.col("s.id") == F.col("t.supplier_id"), "inner")
    .filter(F.col("t.electronics_count") >= 10)
    .select(F.col("s.id"), F.col("s.supplier_name"), F.col("t.electronics_count"))
    .orderBy("electronics_count", "id")
    .limit(1)
)
