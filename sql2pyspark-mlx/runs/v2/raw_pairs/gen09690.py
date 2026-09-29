from pyspark.sql import functions as F

supplier_summary = (
    df_products
    .groupBy("supplier_id")
    .agg(F.count(F.when(F.col("brand") == "Acme", 1)).alias("acme_count"))
)

result = (
    df_suppliers.alias("s")
    .join(supplier_summary.alias("t"), F.col("s.id") == F.col("t.supplier_id"), "inner")
    .select(F.col("s.id"), F.col("s.rating"), F.col("t.acme_count"))
    .orderBy("id", "rating")
)
